package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/judwhite/go-svc/svc"
	"github.com/mreiferson/go-options"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/version"
	"github.com/nsqio/nsq/nsqd"
)

type program struct {
	// 确保实例化对象Do方法在多线程环境只运行一次，内部通过互斥锁实现
	once sync.Once
	// nsqd节点
	nsqd *nsqd.NSQD
}

func main() {
	prg := &program{}
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		logFatal("%s", err)
	}
}

// 初始化操作
func (p *program) Init(env svc.Environment) error {
	if env.IsWindowsService() {
		dir := filepath.Dir(os.Args[0])
		return os.Chdir(dir)
	}
	return nil
}

// 启动nsqd服务
func (p *program) Start() error {
	// 初始化参数
	opts := nsqd.NewOptions()

	flagSet := nsqdFlagSet(opts)
	_ = flagSet.Parse(os.Args[1:])

	// 设置随机种子
	rand.Seed(time.Now().UTC().UnixNano())

	// 输出nsqd版本信息
	if flagSet.Lookup("version").Value.(flag.Getter).Get().(bool) {
		fmt.Println(version.String("nsqd"))
		os.Exit(0)
	}

	// 配置文件路径
	var cfg config
	configFile := flagSet.Lookup("config").Value.String()
	if configFile != "" {
		_, err := toml.DecodeFile(configFile, &cfg)
		if err != nil {
			logFatal("failed to load config file %s - %s", configFile, err)
		}
	}
	// 验证配置项
	cfg.Validate()

	options.Resolve(opts, flagSet, cfg)

	// 创建nsqd实例
	nsqd, err := nsqd.New(opts)
	if err != nil {
		logFatal("failed to instantiate nsqd - %s", err)
	}
	p.nsqd = nsqd

	// 加载metadata数据 metadata中存储了topic和channel信息
	err = p.nsqd.LoadMetadata()
	if err != nil {
		logFatal("failed to load metadata - %s", err)
	}

	// 保证重启时能保留topic与channel数据
	err = p.nsqd.PersistMetadata()
	if err != nil {
		logFatal("failed to persist metadata - %s", err)
	}

	go func() {
		// 启动nsqd服务
		err := p.nsqd.Main()
		if err != nil {
			// 发生错误，终止服务
			_ = p.Stop()
			os.Exit(1)
		}
	}()

	return nil
}

// 终止nsqd服务
func (p *program) Stop() error {
	p.once.Do(func() {
		// 终止nsqd服务
		p.nsqd.Exit()
	})
	return nil
}

func logFatal(f string, args ...interface{}) {
	lg.LogFatal("[nsqd] ", f, args...)
}
