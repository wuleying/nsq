package nsqd

import (
	"crypto/md5"
	"crypto/tls"
	"hash/crc32"
	"io"
	"log"
	"os"
	"time"

	"github.com/nsqio/nsq/internal/lg"
)

type Options struct {
	// basic options
	// nsqd节点id
	ID        int64       `flag:"node-id" cfg:"id"`
	// 日志等级
	LogLevel  lg.LogLevel `flag:"log-level"`
	// 日志前缀
	LogPrefix string      `flag:"log-prefix"`
	// log处理器
	Logger    Logger

	// TCP地址
	TCPAddress               string        `flag:"tcp-address"`
	// HTTP地址
	HTTPAddress              string        `flag:"http-address"`
	// HTTPS地址
	HTTPSAddress             string        `flag:"https-address"`
	// 广播地址
	BroadcastAddress         string        `flag:"broadcast-address"`
	// nsq网络拓扑管理服务TCP地址
	NSQLookupdTCPAddresses   []string      `flag:"lookupd-tcp-address" cfg:"nsqlookupd_tcp_addresses"`
	// auth认证服务HTTP地址
	AuthHTTPAddresses        []string      `flag:"auth-http-address" cfg:"auth_http_addresses"`
	// HTTP客户端连接超时时间
	HTTPClientConnectTimeout time.Duration `flag:"http-client-connect-timeout" cfg:"http_client_connect_timeout"`
	// HTTP客户端请求超时时间
	HTTPClientRequestTimeout time.Duration `flag:"http-client-request-timeout" cfg:"http_client_request_timeout"`

	// diskqueue options
	// 数据文件存储路径
	DataPath        string        `flag:"data-path"`
	// 内存队列大小
	MemQueueSize    int64         `flag:"mem-queue-size"`
	MaxBytesPerFile int64         `flag:"max-bytes-per-file"`
	SyncEvery       int64         `flag:"sync-every"`
	// 同步超时时间
	SyncTimeout     time.Duration `flag:"sync-timeout"`

	QueueScanInterval        time.Duration
	QueueScanRefreshInterval time.Duration
	QueueScanSelectionCount  int
	QueueScanWorkerPoolMax   int
	QueueScanDirtyPercent    float64

	// msg and command options
	MsgTimeout    time.Duration `flag:"msg-timeout"`
	MaxMsgTimeout time.Duration `flag:"max-msg-timeout"`
	MaxMsgSize    int64         `flag:"max-msg-size"`
	MaxBodySize   int64         `flag:"max-body-size"`
	MaxReqTimeout time.Duration `flag:"max-req-timeout"`
	ClientTimeout time.Duration

	// client overridable configuration options
	MaxHeartbeatInterval   time.Duration `flag:"max-heartbeat-interval"`
	MaxRdyCount            int64         `flag:"max-rdy-count"`
	MaxOutputBufferSize    int64         `flag:"max-output-buffer-size"`
	MaxOutputBufferTimeout time.Duration `flag:"max-output-buffer-timeout"`
	MinOutputBufferTimeout time.Duration `flag:"min-output-buffer-timeout"`
	OutputBufferTimeout    time.Duration `flag:"output-buffer-timeout"`
	MaxChannelConsumers    int           `flag:"max-channel-consumers"`

	// statsd integration
	StatsdAddress       string        `flag:"statsd-address"`
	StatsdPrefix        string        `flag:"statsd-prefix"`
	StatsdInterval      time.Duration `flag:"statsd-interval"`
	StatsdMemStats      bool          `flag:"statsd-mem-stats"`
	StatsdUDPPacketSize int           `flag:"statsd-udp-packet-size"`

	// e2e message latency
	E2EProcessingLatencyWindowTime  time.Duration `flag:"e2e-processing-latency-window-time"`
	E2EProcessingLatencyPercentiles []float64     `flag:"e2e-processing-latency-percentile" cfg:"e2e_processing_latency_percentiles"`

	// TLS config
	TLSCert             string `flag:"tls-cert"`
	TLSKey              string `flag:"tls-key"`
	TLSClientAuthPolicy string `flag:"tls-client-auth-policy"`
	TLSRootCAFile       string `flag:"tls-root-ca-file"`
	TLSRequired         int    `flag:"tls-required"`
	TLSMinVersion       uint16 `flag:"tls-min-version"`

	// compression
	// 是否启用deflate压缩
	DeflateEnabled  bool `flag:"deflate"`
	// deflate压缩等级[1-9]
	MaxDeflateLevel int  `flag:"max-deflate-level"`
	// 是否启用snappy压缩
	SnappyEnabled   bool `flag:"snappy"`
}

func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	h := md5.New()
	io.WriteString(h, hostname)
	defaultID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

	return &Options{
		ID:        defaultID,
		LogPrefix: "[nsqd] ",
		LogLevel:  lg.INFO,

		TCPAddress:       "0.0.0.0:4150",
		HTTPAddress:      "0.0.0.0:4151",
		HTTPSAddress:     "0.0.0.0:4152",
		BroadcastAddress: hostname,

		NSQLookupdTCPAddresses: make([]string, 0),
		AuthHTTPAddresses:      make([]string, 0),

		HTTPClientConnectTimeout: 2 * time.Second,
		HTTPClientRequestTimeout: 5 * time.Second,

		MemQueueSize:    10000,
		MaxBytesPerFile: 100 * 1024 * 1024,
		SyncEvery:       2500,
		SyncTimeout:     2 * time.Second,

		QueueScanInterval:        100 * time.Millisecond,
		QueueScanRefreshInterval: 5 * time.Second,
		QueueScanSelectionCount:  20,
		QueueScanWorkerPoolMax:   4,
		QueueScanDirtyPercent:    0.25,

		MsgTimeout:    60 * time.Second,
		MaxMsgTimeout: 15 * time.Minute,
		MaxMsgSize:    1024 * 1024,
		MaxBodySize:   5 * 1024 * 1024,
		MaxReqTimeout: 1 * time.Hour,
		ClientTimeout: 60 * time.Second,

		MaxHeartbeatInterval:   60 * time.Second,
		MaxRdyCount:            2500,
		MaxOutputBufferSize:    64 * 1024,
		MaxOutputBufferTimeout: 30 * time.Second,
		MinOutputBufferTimeout: 25 * time.Millisecond,
		OutputBufferTimeout:    250 * time.Millisecond,
		MaxChannelConsumers:    0,

		StatsdPrefix:        "nsq.%s",
		StatsdInterval:      60 * time.Second,
		StatsdMemStats:      true,
		StatsdUDPPacketSize: 508,

		E2EProcessingLatencyWindowTime: time.Duration(10 * time.Minute),

		DeflateEnabled:  true,
		MaxDeflateLevel: 6,
		SnappyEnabled:   true,

		TLSMinVersion: tls.VersionTLS10,
	}
}
