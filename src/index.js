const { PromisedDatabase } = require('promised-sqlite3');
const Tools = require('./lib/tools/Tools');

module.exports = class TestDataGenerator {

	constructor(config = { preFetchLocations: true, preFetchLocationsMax: 10000 }) {
		Object.defineProperty(this, 'sqlite', { value: new PromisedDatabase() });
		Object.defineProperty(this, 'records', { value: new Array() });
		Object.defineProperty(this, 'locations', { value: new Array() });
		Object.defineProperty(this, 'preFetchLocations', { value: Boolean(config.preFetchLocations) });
		Object.defineProperty(this, 'preFetchLocationsMax', { value: parseInt(config.preFetchLocationsMax) });
	}

	// Number of Records
	// Split into households?
	// Rename fields
	// Chance of blank per field
	// Reformat per field
	// Custom fields
	// Chance of incorrectly formatted data per fields
	// Export format (JSON or CSV)
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
		let gender = this.getGender();
		let firstName = await this.getGivenName(gender);
		let lastName = await this.getSurname();
		let location = await this.getLocation();

		return {
			firstName: firstName,
			lastName: lastName,
			gender:	gender,
			location: location
		};
	}

	async getGivenName(gender = this.getGender(true)) {
		const tableNames = { 'male': 'MaleNames', 'female': 'FemaleNames' };
		const row = await this.sqlite.get(`SELECT * FROM ${tableNames[gender==='other'?this.getGender(true):gender]} ORDER BY RANDOM() LIMIT 1`);
		return row.name;
	}

	async getSurname() {
		const row = await this.sqlite.get(`SELECT * FROM Surnames ORDER BY RANDOM() LIMIT 1`);
		return row.name;
	}

	async getLocation(countryCodes = [], provinces = [], cities = []) {
		if (this.locations.length) return this.locations.pop();
		const queryStringBase = 'SELECT PostalCodes.placeName as city, Provinces.name as province, PostalCodes.countryCode as country, PostalCodes.postalCode FROM PostalCodes INNER JOIN Provinces ON PostalCodes.countryCode=Provinces.countryCode AND PostalCodes.admin1Code=Provinces.admin1Code';
		const columnNames = { countryCodes: 'PostalCodes|countryCode', provinces: 'Provinces|name', cities: 'PostalCodes|placeName'};
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

		return this.locations.pop();
	}

	async buildQueryString(base = '', conditionsByColumn = {}, suffix = '', limit = 0) {
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

	async getPhoneNumber() {

	}

	async getEmail(givenName, surname) {

	}

}