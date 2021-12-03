package producer

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type ConfOptions func(*config)

type config struct {
	Frequency  int
	MaxMessage int
}

func WithFrequency(freq int) ConfOptions {
	return func(c *config) {
		c.Frequency = freq
	}
}

func WithMaxMessage(mm int) ConfOptions {
	return func(c *config) {
		c.MaxMessage = mm
	}
}

type Producer struct {
	producer sarama.AsyncProducer

	topic     string
	msgQ      chan *sarama.ProducerMessage
	wg        sync.WaitGroup
	closeChan chan struct{}
}

// NewProducer 构造KafkaProducer
func NewProducer(topic, broker string, options ...ConfOptions) (*Producer, error) {
	conf := &config{
		Frequency:  5,
		MaxMessage: 400,
	}
	for _, o := range options {
		o(conf)
	}
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.NoResponse                                   // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy                             // Compress messages
	config.Producer.Flush.Frequency = time.Duration(conf.Frequency) * time.Millisecond // Flush batches every 500ms
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	p, err := sarama.NewAsyncProducer(strings.Split(broker, ","), config)
	if err != nil {
		return nil, err
	}
	ret := &Producer{
		producer:  p,
		topic:     topic,
		msgQ:      make(chan *sarama.ProducerMessage, conf.MaxMessage),
		closeChan: make(chan struct{}),
	}
	fmt.Println("new kafka producer client success!")
	return ret, nil
}

// Run 运行
func (p *Producer) Run() {

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()

	LOOP:
		for {
			select {
			case m := <-p.msgQ:
				p.producer.Input() <- m
			case err := <-p.producer.Errors():
				if nil != err && nil != err.Msg {
					fmt.Printf("[producer] err=[%s] topic=[%s] key=[%s] val=[%s]", err.Error(), err.Msg.Topic, err.Msg.Key, err.Msg.Value)
				}
			case <-p.closeChan:
				break LOOP
			}

		}
	}()

}

// Close 关闭
func (p *Producer) Close() error {
	close(p.closeChan)
	fmt.Println("[producer] is quiting")
	p.wg.Wait()
	fmt.Println("[producer] quit over")

	return p.producer.Close()
}

// Send 发送消息
func (p *Producer) Send(val string) {
	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		// Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(val),
	}

	select {
	case p.msgQ <- msg:
		return
	default:
		fmt.Printf("[producer] err=[msgQ is full] key=[%s] val=[%s]", msg.Key, msg.Value)
	}
}
