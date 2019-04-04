package model

import (
	"math/rand"
	"syreclabs.com/go/faker"
	"time"
)

var CardTypes = []string{
	faker.CC_VISA,
	faker.CC_MASTERCARD,
	faker.CC_AMERICAN_EXPRESS,
	faker.CC_DINERS_CLUB,
	faker.CC_DISCOVER,
}

var creditCards = map[string]string{
	"visa":             "VISA",
	"mastercard":       "MasterCard",
	"american_express": "AMERICAN EXPRESS",
	"diners_club":      "DINA CLUB",
	"discover":         "DISCOVER",
}

type CreditCardGenertor struct {
}

func NewCreditCardGenertor() *CreditCardGenertor {

	g := new(CreditCardGenertor)

	return g
}

type Card struct {
	CardNumber string
	CardType   string
}

func (creditCard CreditCardGenertor) NextCard() Card {
	rand.Seed(time.Now().Unix())
	n := rand.Int() % len(CardTypes)
	cardKey := CardTypes[n]

	cardNumber := faker.Finance().CreditCard(cardKey)
	cardType := creditCards[cardKey]
	return Card{cardNumber, cardType}
}
