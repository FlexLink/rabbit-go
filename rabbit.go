package rabbit

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"time"
)

// Config is configuration structure
// - Host: 			IP or hostname to RabbitMQ server
// - Exchange:		exchange to subscribe from
// - BindingKey:	key to select messages
// - Verbose:		log verbose
// - Timeout:		deprecated
type Config struct {
	Host       string `json:"host"`
	Exchange   string `json:"exchange"`
	BindingKey string `json:"bindingkey"`
	Verbose    bool   `json:"verbose"`
	Timeout    int    `json:"timeout"`
}

// Rabbit is a client to RabbitMQ
type Rabbit struct {
	conf         *Config
	amqpConn     *amqp.Connection
	amqpChannel  *amqp.Channel
	amqpQueue    *amqp.Queue
	amqpMsgs     *<-chan amqp.Delivery
	disconnected chan *amqp.Error
	subscribers  map[string]func(*amqp.Delivery)
	bindigs		 map[string][]string
	quitAuto     chan bool
	isOpen       bool
	exchanges    []string
}

// NewRabbit is a default constructor for Rabbit type
func NewRabbit(conf *Config) *Rabbit {
	c := &Rabbit{
		conf:         conf,
		disconnected: make(chan *amqp.Error, 2),
		subscribers:  make(map[string]func(*amqp.Delivery), 10),
		bindigs: make(map[string][]string, 0),
		quitAuto:     make(chan bool),
		isOpen:       false,
		exchanges:    []string{},
	}
	return c
}

// IsOpen is a property indicates if connection is open
func (c *Rabbit) IsOpen() bool { return c.isOpen }

func failOnError(err error, msg string) {
	if err != nil {
		log.Printf("[ERROR] %s: %s", msg, err)
	}
}

// AutoConnect is acync methon to establish aand keep connection
func (c *Rabbit) AutoConnect(timeout int) {
	go func(t int) {
		for {
			select {
			case <-c.disconnected:
				c.connect(t)
				c.initConnection()

				c.disconnected = make(chan *amqp.Error, 1)
				c.amqpConn.NotifyClose(c.disconnected)

				// subscribe all recievers
				for key, handler := range c.subscribers {
					c.subscribeWithBindings(key, handler)
//					c.subscribe(handler)
				}
			case <-c.quitAuto:
				log.Println("[INFO] rabbit.autoconnect quit")
				return
			}
		}
	}(timeout)
	c.disconnected <- amqp.ErrClosed
}

func (c *Rabbit) connect(timeout int) {
	var conn *amqp.Connection
	var err error
	for {
		conn, err = amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:5672/", c.conf.Host))
		if err != nil {
			c.isOpen = false
			log.Printf("[ERROR] Not connected to RabbitMQ, %v\n", err)
			log.Printf("[INFO] Reconnecting to RabbitMQ efter %ds ...\n", timeout/1000)
			time.Sleep(time.Duration(timeout) * time.Millisecond)
		} else {
			c.amqpConn = conn
			log.Println("[INFO] RabbitMQ connected")
			return
		}
	}
}

func (c *Rabbit) initConnection() error {
	conn := c.amqpConn

	ch, err := conn.Channel()
	if err != nil {
		failOnError(err, "Failed to open a channel")
		return err
	}

	err = ch.ExchangeDeclare(
		c.conf.Exchange, // name
		"topic",         // type
		true,            // durable
		false,           // auto-deleted
		false,           // internal
		false,           // no-wait
		nil,             // arguments
	)
	if err != nil {
		failOnError(err, "Failed to declare an exchange")
		return err
	}

	q, err := ch.QueueDeclare(
		"",    // name
		true,  // durable
		true,  // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		failOnError(err, "Failed to declare a queue")
		return err
	}

	c.amqpChannel = ch
	c.amqpQueue = &q
	c.isOpen = true

	for _, exch := range c.exchanges {
		err := c.createExtraExchange(exch)
		failOnError(err, "Failed to declare an extra exchange")
	}
	return nil
}

// AddExtraExchange creates new exchange connection
func (c *Rabbit) AddExtraExchange(name string) {
	c.exchanges = append(c.exchanges, name)
}

func (c *Rabbit) createExtraExchange(exchange string) error {
	err := c.amqpChannel.ExchangeDeclare(
		exchange, // name
		"topic",  // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		failOnError(err, "Failed to declare an exchange")
		return err
	}
	return nil
}

// Subscribe register a callback by receiver's name
func (c *Rabbit) Subscribe(receiver string, handler func(*amqp.Delivery)) error {
	c.subscribers[receiver] = handler
	return nil
}

// Subscribe register a callback by receiver's name
func (c *Rabbit) SubscribeWithBindings(bindings []string, receiver string, handler func(*amqp.Delivery)) error {
	c.subscribers[receiver] = handler
	c.bindigs[receiver] = bindings
	return nil
}

func (c *Rabbit) subscribe(handler func(*amqp.Delivery)) error {
	ch := c.amqpChannel
	if ch == nil {
		return errors.New("in Rabbit.subscribe() amqp.Channel is null")
	}

	err := ch.QueueBind(
		c.amqpQueue.Name,  // queue name
		c.conf.BindingKey, // routing key
		c.conf.Exchange,   // exchange
		false,
		nil,
	)
	if err != nil {
		failOnError(err, "Failed to bind a queue")
		return err
	}

	msgs, err := ch.Consume(
		c.amqpQueue.Name, // queue
		"def",     // consumer
		true,             // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)
	if err != nil {
		failOnError(err, "Failed to register a consumer")
	}

	c.amqpMsgs = &msgs

	go func() {
		for d := range *c.amqpMsgs {
			if c.conf.Verbose {
				log.Printf("[DEBUG] reseived: %v\n", d)
			}
			handler(&d)
		}
	}()

	return nil
}

func (c *Rabbit) subscribeWithBindings(receiver string, handler func(*amqp.Delivery)) error {
	ch := c.amqpChannel
	if ch == nil {
		return errors.New("in Rabbit.subscribe() amqp.Channel is null")
	}

	bindings, ok := c.bindigs[receiver]
	if !ok {
		bindings = []string{ c.conf.BindingKey }
	}
	for _, key := range bindings {
		err := ch.QueueBind(
			c.amqpQueue.Name,  	// queue name
			key, 				// routing key
			c.conf.Exchange,   	// exchange
			false,
			nil,
		)
		if err != nil {
			failOnError(err, "Failed to bind a queue")
		}
	}

	msgs, err := ch.Consume(
		c.amqpQueue.Name, // queue
		receiver,     	  // consumer
		true,             // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)
	if err != nil {
		failOnError(err, "Failed to register a consumer")
	}

	c.amqpMsgs = &msgs

	go func() {
		for d := range *c.amqpMsgs {
			if c.conf.Verbose {
				log.Printf("[DEBUG] reseived: %v\n", d)
			}
			handler(&d)
		}
	}()

	return nil
}

// Push publishes key/message to the exchange
func (c *Rabbit) Push(key string, message string, exchange string) error {
	if exchange == "" {
		exchange = c.conf.Exchange
	}

	if c.amqpChannel != nil && c.isOpen {
		err := c.amqpChannel.Publish(exchange, key, false, false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(message),
			},
		)
		if err != nil {
			log.Printf("[ERROR] %s", err)
			return err
		}
		return nil
	}
	c.isOpen = false
	return errors.New("channel is nil")

}

// Unsubscribe cancels subscription to default exchange
func (c *Rabbit) Unsubscribe() {
	if c.amqpChannel != nil {
		if c.amqpQueue != nil {
			c.amqpChannel.Cancel(c.amqpQueue.Name, true)
		}
	}
}

// Close deletes connection
func (c *Rabbit) Close() {
	c.quitAuto <- true
	if c.amqpChannel != nil {
		c.amqpChannel.Close()
	}
	if c.amqpConn != nil {
		c.amqpConn.Close()
	}
}
