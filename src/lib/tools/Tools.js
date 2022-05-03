module.exports = {
	getRandomNumber(min = 0, max = 0, precision = 0) {
		precision = Math.pow(10,precision);
		return (Math.floor((Math.random() * (max - min + 1) + min) * precision) / precision);
	},

	getWeightedRandomInt(min = 0, max = 0) {
		return (Math.round(max / (Math.random() * max + 1)) + (min - 1));
	}
}