package main

import (
	//"fmt"
	"producer"
)

func main() {

	transactionProducer := producer.NewTransactionProducer()

	//fmt.Println(transactionProducer.CreateTransactionInfo())

	transactionProducer.SendTransactionToKafka()

}
