package consumer

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mozhiyun/go-utils/kafka/consumer/common"
	"github.com/mozhiyun/go-utils/kafka/consumer/worker"
)

//ConsumerOptions
type ConsumerOptions func(*Consumer)

//WithPartition 可选参数patition
func WithPartition(n int32) ConsumerOptions {
	return func(c *Consumer) {
		c.Partition = n
	}
}

//WithReplication 可选参数replication
func WithReplication(n int16) ConsumerOptions {
	return func(c *Consumer) {
		c.Replication = n
	}
}

//Consumer
type Consumer struct {
	Broker        []string            //kafka cluster
	Topic         []string            //topic
	Group         string              //group
	Version       string              //kafka 版本
	Partition     int32               //partition 个数，consumer个数不能超过这个数量
	Replication   int16               //replication 副本因子
	ClusterAdmin  sarama.ClusterAdmin // clusterAdmin 集群admin
	Worker        *worker.Worker
	GroupConsumer sarama.ConsumerGroup //group consumer instance
	Ctx           context.Context
	Cancel        context.CancelFunc
}

//NewConsumer 初始化
func NewConsumer(broker, topic []string, group, version string, w *worker.Worker, options ...ConsumerOptions) *Consumer {
	cs := &Consumer{
		Broker:      broker,
		Topic:       topic,
		Group:       group,
		Version:     version,
		Partition:   int32(16),
		Replication: int16(1),
	}
	for _, o := range options {
		o(cs)
	}
	// 获取host名字
	hostName, err := os.Hostname()
	if nil != err {
		hostName = "[sarama]"
	}
	// sarama的logger
	sarama.Logger = log.New(os.Stdout, fmt.Sprintf("[%s]", hostName), log.LstdFlags)

	// 指定kafka版本，一定要支持kafka集群
	version2, err := sarama.ParseKafkaVersion(cs.Version)
	if err != nil {
		panic(err)
	}
	config := sarama.NewConfig()
	config.Version = version2
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.ClientID = hostName
	ca, err := sarama.NewClusterAdmin(cs.Broker, config)
	if nil != err {
		panic(err)
	}
	cs.ClusterAdmin = ca
	// group consumer instance
	groupConsumer, err := sarama.NewConsumerGroup(cs.Broker, cs.Group, config)
	if err != nil {
		panic(err)
	}
	cs.GroupConsumer = groupConsumer
	ctx, cancel := context.WithCancel(context.Background())
	cs.Ctx = ctx
	cs.Cancel = cancel
	cs.Worker = w
	return cs
}

//DeleteTopic 删除topic
func (c *Consumer) DeleteTopic(topic string) error {
	return c.ClusterAdmin.DeleteTopic(topic)
}

//ListTopics 所有topic
func (c *Consumer) ListTopics() (map[string]sarama.TopicDetail, error) {
	return c.ClusterAdmin.ListTopics()
}

//CreatePartitions 创建partitions
func (c *Consumer) CreatePartitions(topic string, count int32, assignment [][]int32, validateOnly bool) error {
	return c.ClusterAdmin.CreatePartitions(topic, count, assignment, validateOnly)
}

//CreateTopic 创建topic
func (c *Consumer) CreateTopic(topic string, detail *sarama.TopicDetail, validateOnly bool) error {
	return c.ClusterAdmin.CreateTopic(topic, detail, validateOnly)
}

//Run 开启消费
func (c *Consumer) Run() {
	detail, err := c.ListTopics()
	if err != nil {
		panic(err)
	}
	for _, v := range c.Topic {
		if d, ok := detail[v]; ok {
			if c.Partition > d.NumPartitions {
				if err := c.CreatePartitions(v, c.Partition, nil, false); err != nil {
					panic(err)
				}
				fmt.Println("alter topic ok", v, c.Partition)
			}
		} else {
			if err := c.CreateTopic(v, &sarama.TopicDetail{NumPartitions: c.Partition, ReplicationFactor: c.Replication}, false); err != nil {
				panic(err)
			}
			fmt.Println("create topic ok", v)
		}
	}
	c.Worker.Run(c.Ctx)
	defer c.Worker.Close()
	consumer := common.NewMyConsumer(c.Ctx, c.Worker)
	go func() {
		for {
			select {
			case <-c.Ctx.Done():
				return
			default:
				err := c.GroupConsumer.Consume(c.Ctx, c.Topic, consumer)
				if err != nil {
					fmt.Printf("[consumer] groupConsumer.Consume error=[%s]", err.Error())
					time.Sleep(time.Second)
				}
			}
		}
	}()
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	//time.Sleep(time.Second * 4)
	sig := <-sigterm
	fmt.Printf("[consumer] os sig=[%v]", sig)

	c.Cancel()
	fmt.Printf("[consumer] cancel")
	if err := c.GroupConsumer.Close(); nil != err {
		fmt.Printf("[main] kafkaClient close error=[%s]", err.Error())
	}
	fmt.Printf("[consumer] beats quit")
}
