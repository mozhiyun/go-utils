package common

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
)

type taskProcessor interface {
	AddTask(key string, val []byte)
}

// MyConsumer 可关闭的带任务处理器的消费者
type MyConsumer struct {
	processor taskProcessor
	ctx       context.Context
}

// NewMyConsumer 构造
func NewMyConsumer(ctx context.Context, p taskProcessor) *MyConsumer {
	c := &MyConsumer{
		processor: p,
		ctx:       ctx,
	}

	return c
}

// Setup 启动
func (consumer *MyConsumer) Setup(s sarama.ConsumerGroupSession) error {
	fmt.Printf("[main] consumer.Setup memberID=[%s]", s.MemberID())
	return nil
}

// Cleanup 当退出时
func (consumer *MyConsumer) Cleanup(s sarama.ConsumerGroupSession) error {
	fmt.Printf("[main] consumer.Cleanup memberID=[%s]", s.MemberID())
	return nil
}

// ConsumeClaim 消费消息
func (consumer *MyConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			index := fmt.Sprintf("%s-%s", message.Topic, message.Value)
			consumer.processor.AddTask(index, message.Value)
			session.MarkMessage(message, "")

		case <-consumer.ctx.Done():
			return nil
		}
	}
}
