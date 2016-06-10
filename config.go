package geetplaban

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"sync"
	"time"
)

const (
	INVALID_VAL_INT    = -1
	INVALID_VAL_STR    = ""
	DEFAULT_LOG_PATH   = "<home>/log"
	DEFAULT_LOG_FILE   = "pipeline.log"
	DEFAULT_HOME       = "/tmp"
	DEFAULT_NUM_ACKER  = 8
	DEFAULT_LOG_BCK    = 10
	LOG_ROLLING_SZ     = 10485760
	MIN_LOG_ROLLING_SZ = 1048576
	MAX_LOG_ROLLING_SZ = 2147483648
	MAX_MAX_UNACKED    = 2000000
	MIN_MAX_UNACKED    = 20
	TIME_OUT           = 120000
	CLUSTER_NAME       = "geetplaban"
	STATS_INTERVAL_SEC = 60
	DEFAULT_LOG_LEVEL  = "info"
	MIN_TICK_MILI_SEC  = 1
	MAX_TICK_MILI_SEC  = 60000
)

var lolevels = [6]string{"debug", "info", "warning", "error", "fatal", "panic"}

type Config struct {
	file   string
	params map[string]interface{}
}

var config *Config
var once sync.Once
var configFile string

func initConfig(file string) {
	fmt.Printf("Initing file %s\n", file)
	b, err := ioutil.ReadFile(file)
	if err != nil {
		panic(err)
	}
	configFile = file
	var output map[string]interface{}
	err = yaml.Unmarshal(b, &output)
	if err != nil {
		panic(err)
	}
	config = &Config{file, output}
}

func GetConfig() *Config {
	return config
}

func (config *Config) GetMaxUnacked() uint32 {
	max_unacked, _ := config.GetIntVal("max_unacked")
	if max_unacked < MIN_MAX_UNACKED {
		max_unacked = MIN_MAX_UNACKED
	}
	if max_unacked > MAX_MAX_UNACKED {
		max_unacked = MAX_MAX_UNACKED
	}
	return uint32(max_unacked)
}

func (config *Config) GetTickMilli() time.Duration {
	tick_milli, _ := config.GetIntVal("tick_milli_second")
	if tick_milli < MIN_TICK_MILI_SEC {
		tick_milli = MIN_TICK_MILI_SEC
	}
	if tick_milli > MAX_TICK_MILI_SEC {
		tick_milli = MAX_TICK_MILI_SEC
	}
	return time.Duration(tick_milli)
}

func (config *Config) GetLogRollSize() int {
	roll_size, _ := config.GetIntVal("log_rolling_size")
	if roll_size < MIN_LOG_ROLLING_SZ {
		roll_size = LOG_ROLLING_SZ
	}
	if roll_size > MAX_LOG_ROLLING_SZ {
		roll_size = LOG_ROLLING_SZ
	}
	return roll_size
}

func (config *Config) GetLogFile() string {
	file, _ := config.GetStrVal("log_file")
	dir, _ := config.GetStrVal("log_path")
	if file == INVALID_VAL_STR {
		file = DEFAULT_LOG_FILE
	}
	if dir == INVALID_VAL_STR {
		dir = DEFAULT_LOG_PATH
	}
	return dir + "/" + file
}

func (config *Config) GetLogLevel() string {
	loglevel, _ := config.GetStrVal("log_level")
	if loglevel == "" {
		loglevel = DEFAULT_LOG_LEVEL
	}
	return loglevel
}

func (config *Config) GetIntVal(key string) (int, bool) {
	val, ok := config.params[key]
	if !ok {
		return INVALID_VAL_INT, false
	}
	return val.(int), true
}

func (config *Config) GetStrVal(key string) (string, bool) {
	val, ok := config.params[key]
	if !ok {
		return INVALID_VAL_STR, false
	}
	return val.(string), true
}
