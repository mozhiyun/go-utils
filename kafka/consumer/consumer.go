package consumer

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/go-utils/kafka/consumer/common"
	"github.com/go-utils/kafka/consumer/worker"
)

type Consumer struct {
	ctx          context.Context
	ClusterAdmin *sarama.ClusterAdmin
	Consumer     common.MyConsumer
	Worker       worker.NewWorker
}
