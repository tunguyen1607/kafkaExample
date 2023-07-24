// import the `Kafka` instance from the kafkajs library
const { Kafka, logLevel } = require("kafkajs")

// the client ID lets kafka know who's producing the messages
const clientId = "my-app"
// we can define the list of brokers in the cluster
const brokers = ["127.0.0.1:9092"]
// this is the topic to which we want to write messages
const topic = "QH_SERVICE_SYNC_DEV_TEST_OHLC"

// initialize a new kafka client and initialize a producer from it
const kafka = new Kafka({ clientId, brokers, logLevel: logLevel.DEBUG })
const producer = kafka.producer({
	ackTimeoutMs: 100,
	// Partitioner type (default = 0, random = 1, cyclic = 2, keyed = 3, custom = 4), default 0
	//= partitionerType: 2
	})

// we define an async function that writes a new message each second
const produce = async () => {
	await producer.connect()
	for(let i = 0; i < 1000; i++ ){
		producer.send({
			topic,
			acks: 1,
			messages: [
				{
					key: '254757969',
					value: JSON.stringify({
						"event": "OHLC_COMPLETED",
						"internalInstrumentCode": 254757969,
						"attributes": null,
						"computedValues": null,
						"quotationValues": null,
						"eventValues": null,
						"contextValues": null,
						"ohlcBar": {
							"volume": i,
							"high": 2930,
							"low": 2930,
							"barStartTime": 1689923640000,
							"interval": 60000,
							"close": 2930,
							"open": 2930,
							"dataComplete": true
						},
						"eventType": "",
						"tradeEventFlags": "",
						"marketTime": 0,
						"tradingStatus": "ReadyToTrade",
						"tradingSessionId": 2,
						"action": "",
						"fosMarketId": 121
					}),
				},
			],
		})
	}
	// after the produce has connected, we start an interval timer
	// await producer.send({
	// 	topic,
	// 	acks: 1,
	// 	messages: [
	// 		{
	// 			key: '254757969',
	// 			value: JSON.stringify({
	// 				"event": "OHLC_COMPLETED",
	// 				"internalInstrumentCode": 254757969,
	// 				"attributes": null,
	// 				"computedValues": null,
	// 				"quotationValues": null,
	// 				"eventValues": null,
	// 				"contextValues": null,
	// 				"ohlcBar": {
	// 					"volume": 83500,
	// 					"high": 2930,
	// 					"low": 2930,
	// 					"barStartTime": 1689923640000,
	// 					"interval": 60000,
	// 					"close": 2930,
	// 					"open": 2930,
	// 					"dataComplete": true
	// 				},
	// 				"eventType": "",
	// 				"tradeEventFlags": "",
	// 				"marketTime": 0,
	// 				"tradingStatus": "ReadyToTrade",
	// 				"tradingSessionId": 2,
	// 				"action": "",
	// 				"fosMarketId": 121
	// 			}),
	// 		},
	// 	],
	// })
}

module.exports = produce
