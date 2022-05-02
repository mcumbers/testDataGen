const { PromisedDatabase } = require('promised-sqlite3');
const Tools = require('./lib/tools/Tools');

module.exports = class TestDataGenerator {

	constructor(config = { preFetchLocations: true, preFetchLocationsMax: 10000, preFetchGivenNames: true, preFetchGivenNamesMax: 10000, preFetchSurnames: true, preFetchSurnamesMax: 10000 }) {
		Object.defineProperty(this, 'sqlite', { value: new PromisedDatabase() });
		Object.defineProperty(this, 'locations', { value: new Array() });
		Object.defineProperty(this, 'maleNames', { value: new Array() });
		Object.defineProperty(this, 'femaleNames', { value: new Array() });
		Object.defineProperty(this, 'unisexNames', { value: new Array() });
		Object.defineProperty(this, 'surnames', { value: new Array() });
		Object.defineProperty(this, 'streetTypes', { value: new Array() });
		Object.defineProperty(this, 'topLevelDomains', { value: new Array() });
		Object.defineProperty(this, 'preFetchLocations', { value: Boolean(config.preFetchLocations) });
		Object.defineProperty(this, 'preFetchLocationsMax', { value: parseInt(config.preFetchLocationsMax) });
		Object.defineProperty(this, 'preFetchGivenNames', { value: Boolean(config.preFetchGivenNames) });
		Object.defineProperty(this, 'preFetchGivenNamesMax', { value: parseInt(config.preFetchGivenNamesMax) });
		Object.defineProperty(this, 'preFetchSurnames', { value: Boolean(config.preFetchSurnames) });
		Object.defineProperty(this, 'preFetchSurnamesMax', { value: parseInt(config.preFetchSurnamesMax) });
	}

	async generateBatch(params = { records: 10 }) {
		await this.openDB();

		const records = [];

		for (let i = 0; i < params.records; i++) {
			records.push(await this.generatePerson());
		}

		await this.sqlite.close();
		return records;
	}

	async openDB() {
		return await this.sqlite.open(__dirname + '/resources/ReferenceData.sqlite');
	}

	async generatePerson() {
		const gender = this.getGender();
		const firstName = await this.getGivenName(gender);
		const lastName = await this.getSurname();
		const birthday = this.getDate();
		const location = await this.getLocation(['CA'], ['British Columbia']);
		const address = await this.getAddress();
		const email = await this.getEmail(firstName, lastName);

		location.address = address;

		return {
			firstName: firstName,
			lastName: lastName,
			gender:	gender,
			birthday: birthday.toLocaleDateString(undefined, { year: 'numeric', month: 'long', day: 'numeric' }),
			address: location,
			email: email
		};
	}

	async getGivenName(gender = this.getGender(true)) {
		const maleChance = ['maleNames', 'maleNames', 'unisexNames'];
		const femaleChance = ['femaleNames', 'femaleNames', 'unisexNames'];
		const otherChance = ['maleNames', 'femaleNames', 'unisexNames'];

		const randomSelector = Tools.getRandomNumber(0,2);

		switch (gender) {
			case 'male': if (this[maleChance[randomSelector]].length) return this[maleChance[randomSelector]].pop(); break;
			case 'female': if (this[femaleChance[randomSelector]].length) return this[femaleChance[randomSelector]].pop(); break;
			case 'other': if (this[otherChance[randomSelector]].length) return this[otherChance[randomSelector]].pop(); break;
		}

		const queryStringBase = 'SELECT * FROM GivenNames';

		// Always get unisex names
		let conditionsByColumn = { gender: ['u'] };
		// If gender is 'other' get both female and male names
		if (gender === 'male' || gender === 'other') conditionsByColumn.gender.push('m');
		if (gender === 'female' || gender === 'other') conditionsByColumn.gender.push('f');

		const queryString = await this.buildQueryString(queryStringBase, conditionsByColumn, 'ORDER BY RANDOM()', this.preFetchGivenNames ? this.preFetchGivenNamesMax : 1);
		
		const data = await this.sqlite.all(queryString);

		for await (const row of data) {
			switch (row.gender) {
				case 'm': this.maleNames.push(row.name); break;
				case 'f': this.femaleNames.push(row.name); break;
				case 'u': this.unisexNames.push(row.name); break;
			}
		}

		return await this.getGivenName(gender);
	}

	async getSurname() {
		if (this.surnames.length) return this.surnames.pop();

		const queryStringBase = 'SELECT * FROM Surnames';
		const queryString = await this.buildQueryString(queryStringBase, {}, 'ORDER BY RANDOM()', this.preFetchSurnames ? this.preFetchSurnamesMax : 1);

		const data = await this.sqlite.all(queryString);

		for await (const row of data) {
			this.surnames.push(row.name);
		}

		return await this.getSurname();
	}

	async getAddress() {
		// Load Street Types into memory if they haven't already been...
		if (!this.streetTypes.length) {

			const queryStringBase = 'SELECT * FROM StreetTypes';
			const queryString = await this.buildQueryString(queryStringBase, {}, 'ORDER BY RANDOM()', 99999);

			const data = await this.sqlite.all(queryString);

			for await (const row of data) {
				this.streetTypes.push(row.name);
			}

		}

		// Generate random street number
		const streetNumber = Tools.getRandomNumber(1,29999);
		// Use a random Surname as street name
		const streetName = await this.getSurname();
		// Pick a random street type
		const streetType = this.streetTypes[Tools.getRandomNumber(0,this.streetTypes.length-1)];

		return `${streetNumber} ${streetName} ${streetType}`;
	}

	async getEmail(firstName = '', lastName = '') {
		// Load Top Level Domains into memory if they haven't already been...
		if (!this.topLevelDomains.length) {

			const queryStringBase = 'SELECT * FROM TopLevelDomains';
			const queryString = await this.buildQueryString(queryStringBase, {}, 'ORDER BY RANDOM()', 99999);

			const data = await this.sqlite.all(queryString);

			for await (const row of data) {
				this.topLevelDomains.push(row.name);
			}

		}

		const randomDomainPattern = Tools.getRandomNumber(1,6);
		const randomPattern = Tools.getRandomNumber(1,9);

		let domain = '';

		switch (randomDomainPattern) {
			case 1: domain = 'gmail.com'; break;
			case 2: domain = 'outlook.com'; break;
			case 3: domain = 'yahoo.com'; break;
			case 4: domain = 'hotmail.com'; break;
			case 5: domain = `${lastName}.${this.topLevelDomains[Tools.getRandomNumber(0,this.topLevelDomains.length-1)]}`; break;
			case 6: domain = `${firstName}${lastName}.${this.topLevelDomains[Tools.getRandomNumber(0,this.topLevelDomains.length-1)]}`; break;
		}

		switch (randomPattern) {
			case 1: return `${firstName}@${domain}`.toLowerCase();
			case 2: return `${firstName}_${lastName}@${domain}`.toLowerCase();
			case 3: return `${firstName}.${lastName}@${domain}`.toLowerCase();
			case 4: return `${lastName}_${firstName}@${domain}`.toLowerCase();
			case 5: return `${lastName}.${firstName}@${domain}`.toLowerCase();
			case 6: return `${firstName}${lastName}@${domain}`.toLowerCase();
			case 7: return `${lastName}${firstName}@${domain}`.toLowerCase();
			case 8: return `${firstName}${Tools.getRandomNumber(0,9999)}@${domain}`.toLowerCase();
			case 9: return `${lastName}${Tools.getRandomNumber(0,9999)}@${domain}`.toLowerCase();
		}

	}

	async getLocation(countryCodes = [], provinces = [], cities = []) {
		if (this.locations.length) return this.locations.pop();
		const queryStringBase = 'SELECT * FROM Locations';
		const columnNames = { countryCodes: 'country', provinces: 'province', cities: 'city'};
		let conditionsByColumn = {};
		
		if (countryCodes.length) {
			conditionsByColumn[columnNames.countryCodes] = [];
			for await (const countryCode of countryCodes) {
				conditionsByColumn[columnNames.countryCodes].push(countryCode);
			}
		}

		if (provinces.length) {
			conditionsByColumn[columnNames.provinces] = [];
			for await (const province of provinces) {
				conditionsByColumn[columnNames.provinces].push(province);
			}
		}

		if (cities.length) {
			conditionsByColumn[columnNames.cities] = [];
			for await (const city of cities) {
				conditionsByColumn[columnNames.cities].push(city);
			}
		}

		const queryString = await this.buildQueryString(queryStringBase, conditionsByColumn, 'ORDER BY RANDOM()', this.preFetchLocations ? this.preFetchLocationsMax : 0);
		const data = await this.sqlite.all(queryString);

		for await (const row of data) {
			this.locations.push(row);
		}

		return await this.getLocation(countryCodes, provinces, cities);
	}

	async buildQueryString(base = '', conditionsByColumn = {}, suffix = '', limit = 1) {
		let queryString = base;

		if (Object.keys(conditionsByColumn).length) {
			queryString += ' WHERE (';
			const conditionStrings = [];
			for await (const columnName of Object.keys(conditionsByColumn)) {
				const parsedColumnName = columnName.replace('|','.');
				let conditionString = '';
				for await (const conditionValue of conditionsByColumn[columnName]) {
					conditionString += `${parsedColumnName} IS "${conditionValue}" OR `;
				}
				conditionStrings.push(conditionString.substring(0, (conditionString.length - ' OR '.length)));
			}
			queryString += conditionStrings.join(') AND (');
			queryString += ')';
		}

		queryString += ` ${suffix}`;

		if (limit) queryString += ` LIMIT ${parseInt(limit)}`;

		return queryString;
	}

	getDate(min = -2208988800000, max = Date.now()) {
		return new Date(Tools.getRandomNumber(min,max));
	}

	getGender(binaryOnly = false) {
		const genders = ['male','female','other'];
		return genders[Tools.getRandomNumber(0,binaryOnly?1:2)];
	}

}