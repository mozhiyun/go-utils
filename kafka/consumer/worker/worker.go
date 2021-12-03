package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

type Handler interface {
	//消息处理方法，接受一个消息
	MsgHandler([]byte)
}

type MsgHandlerFunc func([]byte)

func (f MsgHandlerFunc) MsgHandler(msg []byte) {
	f(msg)
}

type task struct {
	key string
	val []byte
}

type WorkerOptions func(*Worker)

func WithMaxMessage(cache int) WorkerOptions {
	return func(w *Worker) {
		w.msgQ = make(chan *task, cache)
	}
}

func WithThread(thread int) WorkerOptions {
	return func(w *Worker) {
		w.thread = thread
	}
}

func WithTickTime(tt int) WorkerOptions {
	return func(w *Worker) {
		w.tickTime = tt
	}
}

func WithBatchSize(bs int) WorkerOptions {
	return func(w *Worker) {
		w.batchSize = bs
	}
}

// Worker 消息处理器
type Worker struct {
	wg        sync.WaitGroup
	ctx       context.Context
	msgQ      chan *task //通道缓冲默认2048个消息
	handler   Handler    //消息处理接口
	thread    int        //开多少个线程处理消息默认1个线程
	tickTime  int        //批次处理频率默认5000毫秒
	batchSize int        //每批次处理消息数量默认1024
}

// NewWorker 构造
func NewWorker(ctx context.Context, msgHandler Handler, options ...WorkerOptions) *Worker {
	w := &Worker{
		ctx:       ctx,
		msgQ:      make(chan *task, 2048),
		handler:   msgHandler,
		thread:    1,
		tickTime:  5000,
		batchSize: 1024,
	}
	for _, o := range options {
		o(w)
	}
	return w
}

// Run 开始消费
func (w *Worker) Run() {

	// 线程数
	thread := w.thread
	// ticker
	tickTime := time.Duration(w.tickTime) * time.Millisecond
	// 启动
	for i := 0; i < thread; i++ {
		w.wg.Add(1)
		time.Sleep(tickTime / time.Duration(thread))
		go func(idx int) {
			fmt.Printf("[worker] worker[%d] start", idx)
			// ticker
			ticker := time.NewTicker(tickTime)
			defer ticker.Stop()

		LOOP:
			for {
				select {
				case <-w.ctx.Done():
					log.Printf("[worker] worker[%d] is quiting", idx)
					// 要把通道里的全部执行完才能退出
					for {
						if num := w.process(); num > 0 {
							log.Printf("[worker] worker[%d] process batch [%d] when quiting", idx, num)
						} else {
							break LOOP
						}
						time.Sleep(tickTime)
					}

				case <-ticker.C:
					if num := w.process(); num > 0 {
						fmt.Printf("[worker] worker[%d] process batch [%d] ", idx, num)
					}
				}
			}
			fmt.Printf("[worker] worker[%d] stop", idx)
		}(i)
	}
}

// AddTask 实现接口
func (w *Worker) AddTask(key string, val []byte) {
	t := &task{
		key: key,
		val: val,
	}
	w.msgQ <- t
}

// process 处理任务
func (w *Worker) process() int {
	// 每个批次最多w.batchSize个
	total := 0
LOOP:
	for i := 0; i < w.batchSize; i++ {
		// 有任务就加到这个批次，没任务就退出
		select {
		case m := <-w.msgQ:
			w.handler.MsgHandler(m.val)
			total += 1
		default:
			break LOOP
		}
	}
	return total
}

// Close 关闭 需要外面的context关闭，和等待msgQ任务被执行完毕
func (w *Worker) Close() {
	w.wg.Wait()
	if n := len(w.msgQ); n > 0 {
		fmt.Printf("[worker] worker Close remain msg[%d]", n)
	}
}
