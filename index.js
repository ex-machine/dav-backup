'use strict'

const co = require('co');
const got = require('got');
const moment = require('moment');
const spawn = require('cross-spawn');
const webdavFactory = require('webdav');
const yargs = require('yargs');

const TIMEOUT = 12 * 60 * 60 * 1000;
const REQUEST_TIMEOUT = 4 * 60 * 60 * 1000;
const RETRY_DELAY = 10 * 1000;
const DAV_BASE_URLS = {
	box: 'https://dav.box.com/dav',
	yandex: 'https://webdav.yandex.com'
};

module.exports = (args) => {
	const defaultOptions = {
		mysqlHost: 'localhost',
		'mysql-host': 'localhost',
		mysqlPort: 3306,
		'mysql-port': 3306,
		mongoHost: 'localhost',
		'mongo-host': 'localhost',
		mongoPort: 27017,
		'mongo-port': 27017
	};

	const options = parseOptions(args);

	for (const defaultOptionKey of Object.keys(defaultOptions)) {
		// missing yargs args are `undefined`
		if (options[defaultOptionKey] === undefined) {
			options[defaultOptionKey] = defaultOptions[defaultOptionKey];
		}
	}

	const { dirs, name, daysToKeep, retries, dav: davType, davDir, davLogin, davPass, mysqlDb, mongoDb } = options;

	console.time('Execution time');

	process.on('exit', () => {
		console.timeEnd('Execution time');
	});

	setTimeout(() => {
		console.warn('Timeout');
		process.exit(1);
	}, TIMEOUT);

	if (!(dirs && dirs.length) && !mysqlDb && !mongoDb) {
		console.log('No directories or databases to backup');
		process.exit(1);
	}

	const davBaseUrl = DAV_BASE_URLS[davType] || davType;
	const dav = webdavFactory(davBaseUrl, davLogin, davPass);
	const BACKUP_FILENAME_REGEXP = new RegExp(`^backup-${name}_(?:(mysql|mongo)-.+?_|)(\\d{4}-\\d{2}-\\d{2})_(\\d{2}-\\d{2}-\\d{2}-\\d{3})(?:\\.tgz|\\.tar(?:\\.gz|)|\\.sql(?:\\.gz|))$`);

	co(function* () {
		let davDirContents;

		const davDirSegments = davDir.split('/').filter(Boolean);
		let i = 0;

		do {
			// start with no segment
			const segmentedDavDir = '/' + davDirSegments.slice(0, i).join('/');

			try {
				davDirContents = yield dav.getDirectoryContents(segmentedDavDir);
			} catch (err) {
				// create backup dir if doesn't exists
				if (err.status === 404) {
					console.log(`Creating directory: ${segmentedDavDir}`);
					yield dav.createDirectory(segmentedDavDir);
				} else if (err.status === 401) {
					console.warn('Wrong credentials');
					process.exit(1);
				} else {
					throw err;
				}
			}

			i++;
		} while (i <= davDirSegments.length);

		// delete older backups
		if (daysToKeep > 0 && daysToKeep < Infinity) {
			const obsoleteMoment = moment().subtract(moment.duration(daysToKeep, 'days').asMilliseconds(), 'ms');

			for (const { type, lastmod, filename: filePath, basename } of davDirContents) {
				const lastModifiedMoment = moment(lastmod);
				if (type === 'file' && BACKUP_FILENAME_REGEXP.test(basename) && lastModifiedMoment.isBefore(obsoleteMoment)) {
					console.log(`Deleting old backup: ${filePath}`);
					yield dav.deleteFile(filePath);
				}
			}
		}

		function* backup(backupFn, retriesCount = 0) {
			try {
				yield backupFn(davBaseUrl, options);
			} catch (err) {
				console.warn(err);
				const { backupFilename } = err;

				if (backupFilename) {
					const brokenFilePath = `${davDir}/${err.backupFilename}`;
					try {
						console.log(`Deleting broken backup: ${brokenFilePath}`);
						yield dav.deleteFile(brokenFilePath);
					} catch (err) {
						if (err.status === 401) {
							console.warn('Wrong credentials');
						} else if (err.status !== 404) {
							console.warn(err);
						}
					}
				}

				if (retriesCount < retries) {
					retriesCount++;
					console.log(`Retrying`);
					const delayPromise = new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
					yield delayPromise;
					yield* backup(backupFn, retriesCount);
				}
			}
		}

		// backup directories
		if (dirs && dirs.length) {
			yield* backup(backupTar);
			console.log('Directory dump completed');
		}

		// backup MySQL database
		if (mysqlDb) {
			yield* backup(backupMysql);
			console.log('MySQL dump completed');
		}

		// backup Mongo database
		if (mongoDb) {
			yield* backup(backupMongo);
			console.log('MongoDB dump completed');
		}
	})
	.then(() => {
		process.exit(0);
	})
	.catch(err => {
		console.error(err);
		process.exit(1);
	});
};

function backupTar(davBaseUrl, options) {
	const { dirs, excludeDirs, name, davDir, davLogin, davPass } = options;

	const backupPromise = new Promise((resolve, reject) => {
		const backupFilename = `backup-${name}_` + moment().format('YYYY-MM-DD_HH-mm-ss-SSS') + '.tgz';
		const excludeDirsArgs = (excludeDirs && excludeDirs.length)
			? excludeDirs.map(dir => `--exclude="${dir}"`)
			: [];

		// tar --create --gzip --absolute-names
		const tarProcess = spawn('tar', ['--create', '--gzip', '--absolute-names', '--warning=no-file-changed', '--warning=no-file-removed', ...excludeDirsArgs, ...dirs], { stdio: ['ignore', 'pipe', 'inherit'] });

		tarProcess.on('error', reject);
		tarProcess.on('exit', (code, _signal) => {
			// ignore warnings
			if (code > 1) {
				const err = new Error(`Directory backup failed with code ${code}`);
				err.backupFilename = backupFilename;
				reject(err);
			}
		});

		const tarStream = tarProcess.stdout;
		const credentials = new Buffer(`${davLogin}:${davPass}`).toString('base64');
		const putUrl = `${davBaseUrl}${davDir}/${backupFilename}`;

		console.log(`Uploading: ${putUrl}`);
		const putRequestStream = got.stream.put(putUrl, {
			headers: { Authorization: `Basic ${credentials}` },
			timeout: REQUEST_TIMEOUT,
			followRedirect: true // avoid 302 Found errors
		});

		putRequestStream.on('error', err => {
			if (err.statusCode === 507) {
				console.warn('Not enough storage space');
				process.exit(1);
			} else {
				reject(err);
			}
		});

		// should wait for response
		putRequestStream.on('response', ({ statusCode, statusMessage }) => {
			if (statusCode === 201) {
				resolve();
			} else {
				const err = new Error(`Directory backup upload failed with ${statusCode} ${statusMessage}`);
				err.backupFilename = backupFilename;
				reject(err);
			}
		});

		tarStream.pipe(putRequestStream);
	});


	return backupPromise;
}

function backupMysql(davBaseUrl, options) {
	const { name, davDir, davLogin, davPass, mysqlDb, mysqlHost, mysqlPort, mysqlLogin, mysqlPass } = options;

	const backupPromise = new Promise((resolve, reject) => {
		const backupFilename = `backup-${name}_mysql-${mysqlDb}_` + moment().format('YYYY-MM-DD_HH-mm-ss-SSS') + '.sql.gz';

		const mysqlPassArg = mysqlPass ? `--password=${mysqlPass}` : '';

		// TODO: stderr Warning: Using a password on the command line interface can be insecure
		const mysqldumpProcess = spawn(
			[
				`mysqldump --databases ${mysqlDb} --host=${mysqlHost} --port=${mysqlPort} --user=${mysqlLogin} ${mysqlPassArg}`,
				`gzip --stdout -9`
			].join(' | '),
			{
				shell: true,
				stdio: ['ignore', 'pipe', 'inherit']
			}
		);

		mysqldumpProcess.on('error', reject);
		mysqldumpProcess.on('exit', (code, _signal) => {
			if (code) {
				const err = new Error(`Database backup failed with code ${code}`);
				err.backupFilename = backupFilename;
				reject(err);
			}
		});

		const mysqlStream = mysqldumpProcess.stdout;

		const credentials = new Buffer(`${davLogin}:${davPass}`).toString('base64');
		const putUrl = `${davBaseUrl}${davDir}/${backupFilename}`;

		console.log(`Uploading: ${putUrl}`);
		const putRequestStream = got.stream.put(putUrl, {
			headers: { Authorization: `Basic ${credentials}` },
			timeout: REQUEST_TIMEOUT,
			followRedirect: true // avoid 302 Found errors
		});

		putRequestStream.on('error', err => {
			if (err.statusCode === 507) {
				console.warn('Not enough storage space');
				process.exit(1);
			} else {
				reject(err);
			}
		});

		// should wait for response
		putRequestStream.on('response', ({ statusCode, statusMessage }) => {
			if (statusCode === 201) {
				resolve();
			} else {
				const err = new Error(`Database backup upload failed with ${statusCode} ${statusMessage}`);
				err.backupFilename = backupFilename;
				reject(err);
			}
		});

		mysqlStream.pipe(putRequestStream);
	});

	return backupPromise;
}

function backupMongo(davBaseUrl, options) {
	const { name, davDir, davLogin, davPass, mongoDb, mongoHost, mongoPort, mongoLogin, mongoPass } = options;

	const backupPromise = new Promise((resolve, reject) => {
		const backupFilename = `backup-${name}_mongo-${mongoDb}_` + moment().format('YYYY-MM-DD_HH-mm-ss-SSS') + '.sql.gz';

		const mongoLoginArg = mongoLogin ? `--username=${mongoLogin}` : '';
		const mongoPassArg = mongoPass ? `--password=${mongoPass}` : '';

		const mongodumpProcess = spawn(
			`mongodump`,
			[`--quiet`, `--archive`, `--gzip`, `--db=${mongoDb}`, `--host=${mongoHost}`, `--port=${mongoPort}`, ...[mongoLoginArg, mongoPassArg].filter(Boolean)],
			{  stdio: ['ignore', 'pipe', 'inherit'] }
		);

		mongodumpProcess.on('error', reject);
		mongodumpProcess.on('exit', (code, _signal) => {
			if (code) {
				const err = new Error(`Database backup failed with code ${code}`);
				err.backupFilename = backupFilename;
				reject(err);
			}
		});

		const mongoStream = mongodumpProcess.stdout;

		const credentials = new Buffer(`${davLogin}:${davPass}`).toString('base64');
		const putUrl = `${davBaseUrl}${davDir}/${backupFilename}`;

		console.log(`Uploading: ${putUrl}`);
		const putRequestStream = got.stream.put(putUrl, {
			headers: { Authorization: `Basic ${credentials}` },
			timeout: REQUEST_TIMEOUT,
			followRedirect: true // avoid 302 Found errors
		});

		putRequestStream.on('error', err => {
			if (err.statusCode === 507) {
				console.warn('Not enough storage space');
				process.exit(1);
			} else {
				reject(err);
			}
		});

		// should wait for response
		putRequestStream.on('response', ({ statusCode, statusMessage }) => {
			if (statusCode === 201) {
				resolve();
			} else {
				const err = new Error(`Database backup upload failed with ${statusCode} ${statusMessage}`);
				err.backupFilename = backupFilename;
				reject(err);
			}
		});

		mongoStream.pipe(putRequestStream);
	});

	return backupPromise;
}

function parseOptions(args) {
	const options = yargs(args)
	.option('name', {
		type: 'string',
		check: name => /^[a-z0-9]+$/i.test(name),
		demandOption: true,
		desc: 'Alphanumeric'
	})
	.option('daysToKeep', {
		type: 'number',
		default: Infinity
	})
	.option('retries', {
		type: 'number',
		default: 1
	})
	.option('dirs', {
		type: 'array'
	})
	.option('excludeDirs', {
		type: 'array',
		implies: 'dirs'
	})
	.option('dav', {
		type: 'string',
		choices: ['box', 'yandex'],
		implies: 'davLogin',
		demandOption: true
	})
	.option('davDir', {
		type: 'string',
		default: '/backup',
		coerce: path => '/' + path.replace(/^\/|\/$/, ''),
		implies: 'dav'
	})
	.option('davLogin', {
		type: 'string',
		demandOption: true,
		implies: 'davPass'
	})
	.option('davPass', {
		type: 'string',
		demandOption: true
	})
	.option('mysqlDb', {
		type: 'string',
		implies: 'mysqlLogin'
	})
	.option('mysqlLogin', {
		type: 'string',
		implies: 'mysqlDb'
	})
	.option('mysqlPass', {
		type: 'string',
		implies: 'mysqlDb'
	})
	.option('mysqlHost', {
		type: 'string',
		implies: 'mysqlDb'
	})
	.option('mysqlPort', {
		type: 'number',
		implies: 'mysqlDb'
	})
	.option('mongoDb', {
		type: 'string'
	})
	.option('mongoLogin', {
		type: 'string',
		implies: 'mongoDb'
	})
	.option('mongoPass', {
		type: 'string',
		implies: 'mongoDb'
	})
	.option('mongoHost', {
		type: 'string',
		implies: 'mongoDb'
	})
	.option('mongoPort', {
		type: 'number',
		implies: 'mongoDb'
	})
	.help()
	.argv;

	return options;
}
