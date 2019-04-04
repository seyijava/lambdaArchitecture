package producer

import (
	//"bufio"
	"encoding/json"
	"fmt"
	//"github.com/bxcodec/faker"
	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
	"math"
	"math/rand"
	"model"
	"strconv"
	"time"
	//"os"
	//"strings"
)

const (
	topic     = "creditCardTxTopic"
	partition = 1
)

type TransactionProducer struct {
}

var kafkaAddrs = []string{"192.168.2.24:9092"}

type TransactionInfo struct {
	Amount          float64 `json:"amount"`
	Merchant        string  `json:"merchant"`
	CardNumber      string  `json:"cardNumber"`
	CardType        string  `json:"cardType"`
	TransactionDate string  `json:"transactionDate"`
	Status          string  `json:"status"`
	TransactionId   string  `json:"transactionId"`
	Country         string  `json:"country"`
}

type TransactionDetails struct {
	Latitude         float32 `faker:"lat"`
	Longitude        float32 `faker:"long"`
	CreditCardNumber string  `faker:"cc_number"`
	CreditCardType   string  `faker:"cc_type"`
}

func NewTransactionProducer() *TransactionProducer {
	transactionProducer := new(TransactionProducer)
	return transactionProducer
}

func newTransactionInfo() *TransactionInfo {
	transactionInfo := new(TransactionInfo)
	return transactionInfo
}

func randTransactionAmt(min, max float64) float64 {
	rand.Seed(time.Now().UnixNano())
	res := min + rand.Float64()*(max-min)
	return math.Floor(res*100) / 100

}

func createTransactionInfo() (string, error) {

	merchant := model.NewMerchant()
	location := model.NewLocation()
	cardGenerator := model.NewCreditCardGenertor()

	var nextMarchant = merchant.NextMerchant()
	var nextLocation = location.NextLocation()
	var card = cardGenerator.NextCard()
	transactionInfo := newTransactionInfo()

	min := 1.00
	max := 100000.00
	transactionInfo.Amount = randTransactionAmt(min, max)
	transactionInfo.Merchant = nextMarchant
	transactionInfo.Country = nextLocation.Country
	transactionInfo.CardNumber = card.CardNumber
	transactionInfo.Status = strconv.Itoa(random(1, 5))
	transactionInfo.CardType = card.CardType
	currentTime := time.Now()
	transactionInfo.TransactionDate = currentTime.Format("2006-01-02 3:4:5")
	payload, err := json.Marshal(transactionInfo)

	return string(payload), err
}

func (transaction TransactionProducer) SendTransactionToKafka() {

	conf := kafka.NewBrokerConf("test-client")
	conf.AllowTopicCreation = true

	broker, err := kafka.Dial(kafkaAddrs, conf)
	if err != nil {
		fmt.Printf("cannot connect to kafka cluster: %s", err)
	}
	defer broker.Close()
	ProduceMessageToKafka(broker)

}

func random(min, max int) int {
	rand.Seed(time.Now().UnixNano())
	n := min + rand.Intn(max-min+1)
	return n
}

func ProduceMessageToKafka(broker kafka.Client) {
	conf := kafka.NewProducerConf()
	conf.RequiredAcks = proto.RequiredAcksLocal
	producer := broker.Producer(conf)

	for {
		transactionPayload, err := createTransactionInfo()

		if err != nil {
			fmt.Println(fmt.Sprintf("cannot connect to kafka cluster [%s]", err))

		}

		msg := &proto.Message{Value: []byte(transactionPayload)}

		offval, err := producer.Produce(topic, 0, msg)

		if err != nil {
			fmt.Println(fmt.Sprintf("cannot produce message to [%s] [%s] [%s]", topic, partition, err))
		} else {
			fmt.Println()
			fmt.Println(fmt.Sprintf("offset [%d] Message Sent  [%s]", offval, transactionPayload))
		}

	}
}
