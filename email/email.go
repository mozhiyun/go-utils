package email

import (
	"crypto/tls"
	"fmt"
	"net/smtp"
	"strings"

	"github.com/jordan-wright/email"
)

type Email struct {
	To       string `mapstructure:"to" json:"to" yaml:"to"`
	Port     int    `mapstructure:"port" json:"port" yaml:"port"`
	From     string `mapstructure:"from" json:"from" yaml:"from"`
	Host     string `mapstructure:"host" json:"host" yaml:"host"`
	IsSSL    bool   `mapstructure:"is-ssl" json:"isSSL" yaml:"is-ssl"`
	Secret   string `mapstructure:"secret" json:"secret" yaml:"secret"`
	Nickname string `mapstructure:"nickname" json:"nickname" yaml:"nickname"`
}

//@author: [maplepie](https://github.com/maplepie)
//@function: Email
//@description: Email发送方法
//@param: subject string, body string
//@return: error

func (em *Email) Email(subject string, body string) error {
	to := strings.Split(em.To, ",")
	return em.send(to, subject, body)
}

//@author: [SliverHorn](https://github.com/SliverHorn)
//@function: ErrorToEmail
//@description: 给email中间件错误发送邮件到指定邮箱
//@param: subject string, body string
//@return: error

func (em *Email) ErrorToEmail(subject string, body string) error {
	to := strings.Split(em.To, ",")
	if to[len(to)-1] == "" { // 判断切片的最后一个元素是否为空,为空则移除
		to = to[:len(to)-1]
	}
	return em.send(to, subject, body)
}

//@author: [maplepie](https://github.com/maplepie)
//@function: EmailTest
//@description: Email测试方法
//@param: subject string, body string
//@return: error

func (em *Email) EmailTest(subject string, body string) error {
	to := []string{em.From}
	return em.send(to, subject, body)
}

//@author: [maplepie](https://github.com/maplepie)
//@function: send
//@description: Email发送方法
//@param: subject string, body string
//@return: error

func (em *Email) send(to []string, subject string, body string) error {
	from := em.From
	nickname := em.Nickname
	secret := em.Secret
	host := em.Host
	port := em.Port
	isSSL := em.IsSSL

	auth := smtp.PlainAuth("", from, secret, host)
	e := email.NewEmail()
	if nickname != "" {
		e.From = fmt.Sprintf("%s <%s>", nickname, from)
	} else {
		e.From = from
	}
	e.To = to
	e.Subject = subject
	e.HTML = []byte(body)
	var err error
	hostAddr := fmt.Sprintf("%s:%d", host, port)
	if isSSL {
		err = e.SendWithTLS(hostAddr, auth, &tls.Config{ServerName: host})
	} else {
		err = e.Send(hostAddr, auth)
	}
	return err
}
