const fs = require('fs');
const readline = require('readline');
const nthline = require('nthline');

const indices = {
	// donors_state: {},
	donor_ids: {},
	donor_ids_in_donations: {}
}

const prefixLength = 5;

// Читаем построчно файл CSV
// Я для скорости не стал заморачиваться на побайтный оффсет,
// плюс здесь естественный разделитель перенос строки
function prepareIndex(tableName, processors) {
	return new Promise(resolve => {
		let lineNumber = 0;
		
		const input = fs.createReadStream(`${ tableName }.csv`);
		const rl = readline.createInterface({
		    input,
		    output: process.stdout,
		    terminal: false
		});

		rl.on('line', line => {
			if (lineNumber > 0) {
				
				// Close stream not to wait until whole file is read
				/*
				if (lineNumber > 100000) {

					input.close();
					resolve(true);
					return;
				}
				*/
				

				processors.forEach(fn => fn(line, lineNumber));
			}
			lineNumber++;
		});

		rl.on('close', () => resolve(true));
	});
};


// Загружаем по префиксу 2 блока из двух таблиц, связанных через внешний ключ
async function handlePrefixBunch (prefix, tableConfigs, commitBunch, commonAccumulator) {
	const bunchAccumulator = {
		[ tableConfigs[0].tableName ]: {},
		[ tableConfigs[1].tableName ]: {},
	};
	await Promise.all(tableConfigs
		.map(async tableConfig => indices[ tableConfig.indexKey  ][ prefix ] && await Promise.all(await indices[ tableConfig.indexKey  ][ prefix ]
			.map(async lineNumber => {
				try {
					tableConfig.parseLine (
						await nthline(lineNumber, `${ tableConfig.tableName }.csv`),
						bunchAccumulator
					)
				} catch(e) {
					console.log('handlePrefixBunch error, lineNumber', lineNumber, e.message);
				}
			}))
		)
	);
	commitBunch(commonAccumulator, bunchAccumulator);
}

// Функция для полного прохода через тело таблицы по частям,
// закрепленным за теми или иными префиксами индекса
async function goThroughTwoJoinedTables(tableConfigs, commitBunch) {

	console.log('Starting joining and grouping');

	const commonAccumulator = {};
	await Promise.all(
		Object.keys(indices [ tableConfigs[ 0 ].indexKey ])
			// .slice(0,5)
			.map(async prefix => await handlePrefixBunch(prefix, tableConfigs, commitBunch, commonAccumulator))
	);
	return commonAccumulator;
}


// Подготовка индексов для для выборки на их базе
async function prepare() {

	console.log('Getting indices prepared');

	// В индекс indices.donor_ids добавляем списки линий по префиксу длиной prefixLength символа
	// Размер 39 мб
	// Время создания 12 сек

	await prepareIndex(
		'donors',
		[
			(line, lineNumber) => {
				const fieldValue = line.split(',')[0];
				const prefix = fieldValue.substring(0, prefixLength);
				if (!indices[ 'donor_ids' ][ prefix ]) {
					indices[ 'donor_ids' ][ prefix ] = [];
				}
				indices[ 'donor_ids' ][ prefix ].push(lineNumber);
			}
		]
	);
	console.log('1 index size ==>', JSON.stringify(indices).length/1024/1024);


	
	// Аналогичный индекс по donor_id в donations
	// Время создания 20 сек
	// Размер 60 мб

	await prepareIndex(
		'donations',
		[
			(line, lineNumber) => {
				const fieldValue = line.split(',')[2];
				const prefix = fieldValue.substring(0, prefixLength);
				if (!indices[ 'donor_ids_in_donations' ][ prefix ]) {
					indices[ 'donor_ids_in_donations' ][ prefix ] = [];
				}
				indices[ 'donor_ids_in_donations' ][ prefix ].push(lineNumber);
			}
		]
	);
	console.log('2 index size ==>', JSON.stringify(indices).length/1024/1024);

}

// Главная функция
async function main() {

	// Подготовка индексов

	await prepare();

 	// Выборка

	const result = await goThroughTwoJoinedTables(
		// Два конфига как читать банчи по схожим префиксам
		[
			{
				indexKey:  'donor_ids',
				tableName: 'donors',
				parseLine: (line, bunchAccumulator) => {
					// Extract donor id and donor state from line
					const fields = line.split(',');
					bunchAccumulator['donors'][ fields[0] ] = fields[2];
				}
			},
			{
				indexKey:  'donor_ids_in_donations',
				tableName: 'donations',
				parseLine: (line, bunchAccumulator) => {
					// Extract donor id and donation amount from line
					const fields = line.split(',');
					if (!bunchAccumulator[ 'donations' ][ fields[2] ]) {
						bunchAccumulator[ 'donations' ][ fields[2] ] = 0;
					}
					bunchAccumulator[ 'donations' ][ fields[2] ] += parseInt(fields[4]);
				}
			},
		],

		// Что делаем с каждым банчем после чтения
		(commonAccumulator, bunchAccumulator) => {
			Object.keys(bunchAccumulator[ 'donors' ])
				.forEach(userIdKey => {
					try {
						// Добавляем сумму аммаунтов этого
						// пользователя в штат этого пользователя:
						const state = bunchAccumulator['donors'][userIdKey];
						const donationAmount = bunchAccumulator['donations'][userIdKey] || 0;
						commonAccumulator[ state ] = 
							typeof commonAccumulator[ state ] === 'undefined'
								? donationAmount
								: commonAccumulator[ state ] + donationAmount;
					} catch(e) {
						console.error('error 1=>', bunchAccumulator['donors'][userIdKey]);
						console.error('error 2=>', userIdKey)
					}
				})
		}
	);

	console.log('result', result);

}

main();
