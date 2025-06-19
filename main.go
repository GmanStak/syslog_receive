package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql" // 导入 MySQL 驱动程序
	"gopkg.in/yaml.v2"
)

// 定义配置结构
type Config struct {
	Port     int    `yaml:"port"`
	SendAddr string `yaml:"sendaddr"`
	SendPort int    `yaml:"sendport"`
	RulesDir string `yaml:"rulesdir"`
	Mod      string `yaml:"mod"`
}

// 定义数据库配置结构
type DBConfig struct {
	MyAddr     string `yaml:"myaddr"`
	MyPort     int    `yaml:"myport"`
	MyUser     string `yaml:"myuser"`
	MyPasswd   string `yaml:"mypasswd"`
	MyDatabase string `yaml:"mydatabase"`
	Enable     string `yaml:"enable"`
}

// 定义规则结构
type Rule struct {
	Logic []Logic `json:"logic"`
}

type Logic struct {
	Condition string `json:"condition"`
	Message   string `json:"message"`
	Level     string `json:"level"`
	Include   string `json:"include,omitempty"`
}

var db *sql.DB
var mutex sync.Mutex

// 初始化数据库连接
func init() {
	dbFile := flag.String("dbconfig", "db.yaml", "Path to the database configuration file")
	flag.Parse()

	// 读取数据库配置文件
	dbConfigData, err := ioutil.ReadFile(*dbFile)
	if err != nil {
		log.Fatalf("Failed to read db config file: %v", err)
	}

	var dbConfig DBConfig
	err = yaml.Unmarshal(dbConfigData, &dbConfig)
	if err != nil {
		log.Fatalf("Failed to parse db config file: %v", err)
	}

	// 根据配置连接数据库
	if strings.ToLower(dbConfig.Enable) == "true" {
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", dbConfig.MyUser, dbConfig.MyPasswd, dbConfig.MyAddr, dbConfig.MyPort, dbConfig.MyDatabase)
		var err error
		db, err = sql.Open("mysql", dsn)
		if err != nil {
			log.Fatalf("Failed to connect to MySQL: %v", err)
		}
		if err := db.Ping(); err != nil {
			log.Fatalf("Failed to ping MySQL: %v", err)
		}
		log.Println("Successfully connected to MySQL")
	} else {
		log.Println("Database write is disabled.")
	}
}

// 用于存储日志的函数
func storeLocalLog(message string, level string) {
	currentTime := time.Now().Format("2006-01-02 15:04:05") // 获取当前时间并格式化
	file, err := os.OpenFile("syslog.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open syslog.log: %v", err)
	}
	defer file.Close()

	_, err = file.WriteString(fmt.Sprintf("%s - %s - %s: %s\n", currentTime, level, "syslog", message))
	if err != nil {
		log.Printf("Failed to write to syslog.log: %v", err)
	}
}

// 用于转发远程日志的函数
func sendRemoteLog(message string, level string, addr string, port int) {
	if addr == "" {
		return
	}

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", addr, port))
	if err != nil {
		log.Printf("Failed to connect to remote syslog server: %v", err)
		return
	}
	defer conn.Close()

	_, err = conn.Write([]byte(fmt.Sprintf("%s - %s: %s\n", level, "syslog", message)))
	if err != nil {
		log.Printf("Failed to send to remote syslog server: %v", err)
	}
}

// 加载规则文件，支持 include
func loadRules(rulesFilePath string) (Rule, error) {
	data, err := ioutil.ReadFile(rulesFilePath)
	if err != nil {
		return Rule{}, fmt.Errorf("failed to read rules file: %v", err)
	}

	var rule Rule
	err = json.Unmarshal(data, &rule)
	if err != nil {
		return Rule{}, fmt.Errorf("failed to parse rules file: %v", err)
	}

	// 处理 include 语句
	for _, logic := range rule.Logic {
		if logic.Include != "" {
			// 构造被包含文件的路径
			rulesDir := filepath.Dir(rulesFilePath)
			includePath := filepath.Join(rulesDir, logic.Include)
			// 递归加载被包含的规则文件
			includeRule, err := loadRules(includePath)
			if err != nil {
				return Rule{}, err
			}
			rule.Logic = append(rule.Logic, includeRule.Logic...)
		}
	}

	return rule, nil
}

// 解析逻辑规则
func evalLogic(logic []Logic, parts []string) (string, string) {
	for _, rule := range logic {
		// 跳过包含 include 的规则
		if rule.Include != "" {
			continue
		}

		var condition bool
		if rule.Condition == "default" {
			condition = true
		} else {
			conditionStr := replaceVariables(rule.Condition, parts)
			condition = evalCondition(conditionStr)
		}

		if condition {
			message := replaceVariables(rule.Message, parts)
			level := replaceVariables(rule.Level, parts)
			return message, level
		}
	}

	// 默认值
	return strings.Join(parts, " "), "info"
}

// 替换变量
func replaceVariables(template string, match []string) string {
	for i, val := range match {
		template = strings.ReplaceAll(template, fmt.Sprintf("$%d", i), val)
	}
	return template
}

// 匹配正则表达式
func wildcardToRegex(pattern string) string {
	pattern = strings.Replace(pattern, ".", "\\.", -1) // 转义点字符
	pattern = strings.Replace(pattern, "*", ".*", -1)  // 将通配符 * 替换为正则表达式的 .*
	pattern = strings.Replace(pattern, "?", ".", -1)   // 将通配符 ? 替换为正则表达式的 .
	return "^" + pattern + "$"
}

// 左右字符串对比,rithstr为模糊匹配字段
func regex_strings(lestr string, rithstr string) bool {

	// 将通配符表达式转换为正则表达式
	regexPattern := wildcardToRegex(rithstr)

	// 编译并匹配
	re, err := regexp.Compile(regexPattern)
	if err != nil {
		fmt.Println("正则表达式编译错误:", err)
		return false
	}

	isMatch := re.MatchString(lestr)
	fmt.Printf("字符串 '%s' 是否匹配通配符表达式 '%s': %v\n", lestr, rithstr, isMatch)
	return isMatch
}

// 评估条件
func evalCondition(conditionStr string) bool {
	parts := strings.Split(conditionStr, "==")
	if len(parts) != 2 {
		return false
	}
	left := strings.TrimSpace(parts[0])
	right := strings.TrimSpace(parts[1])
	return regex_strings(left, right)
}

// 使用规则解析 syslog 消息
func parseSyslogMessage(message string, logic []Logic) (string, string) {
	parts := strings.Fields(message)
	newMessage, level := evalLogic(logic, parts)
	return newMessage, level
}

// 将日志写入 MySQL 数据库
func storeToMySQL(message string, level string) {
	// 获取当前时间并格式化
	currentTime := time.Now().Format("2006-01-02 15:04:05")
	if db == nil {
		log.Println("Database write is disabled.")
		return
	}

	// 使用互斥锁确保数据库操作的线程安全
	mutex.Lock()
	defer mutex.Unlock()

	_, err := db.Exec("INSERT INTO syslog (currentTime, level,message) VALUES (?, ?, ?)", currentTime, level, message)
	if err != nil {
		log.Printf("Failed to insert data into MySQL: %v", err)
	}
}

// 处理单个 syslog 消息
func handleSyslogMessage(message string, rule Rule) {
	log.Printf("Received message: %s", message)

	parsedMessage, level := parseSyslogMessage(message, rule.Logic)
	log.Printf("Parsed message: %s, Level: %s", parsedMessage, level)

	// 存储到本地日志文件
	storeLocalLog(parsedMessage, level)

	// 存储到 MySQL 数据库（如果启用了）
	storeToMySQL(parsedMessage, level)

	// 如果有远程转发地址，则进行远程转发
	// 这里省略了远程转发的代码，可以根据需要添加
}

func main() {
	configFile := flag.String("config", "config.yaml", "Path to the configuration file")
	flag.Parse()

	// 读取配置文件
	configData, err := ioutil.ReadFile(*configFile)
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	var config Config
	err = yaml.Unmarshal(configData, &config)
	if err != nil {
		log.Fatalf("Failed to parse config file: %v", err)
	}

	// 加载规则文件
	rulesFilePath := filepath.Join(config.RulesDir, "syslog.rules")
	rule, err := loadRules(rulesFilePath)
	if err != nil {
		log.Fatalf("Failed to load rules: %v", err)
	}

	log.SetOutput(os.Stdout)

	switch config.Mod {
	case "tcp":
		addr := fmt.Sprintf(":%d", config.Port)
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatalf("Failed to listen on TCP port %d: %v", config.Port, err)
		}
		defer listener.Close()

		log.Printf("Syslog server started on TCP port %d", config.Port)

		var wg sync.WaitGroup
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("Failed to accept TCP connection: %v", err)
				continue
			}

			wg.Add(1)
			go func(conn net.Conn) {
				defer wg.Done()
				defer conn.Close()

				buffer := make([]byte, 2048)
				n, err := conn.Read(buffer)
				if err != nil {
					log.Printf("Failed to read TCP message: %v", err)
					return
				}

				message := strings.TrimSpace(string(buffer[:n]))
				handleSyslogMessage(message, rule)
			}(conn)
		}

	case "udp":
		addr := fmt.Sprintf(":%d", config.Port)
		udpAddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			log.Fatalf("Failed to resolve UDP address: %v", err)
		}

		conn, err := net.ListenUDP("udp", udpAddr)
		if err != nil {
			log.Fatalf("Failed to listen on UDP port %d: %v", config.Port, err)
		}
		defer conn.Close()

		log.Printf("Syslog server started on UDP port %d", config.Port)

		var wg sync.WaitGroup
		for {
			buffer := make([]byte, 2048)
			n, src, err := conn.ReadFromUDP(buffer)
			if err != nil {
				log.Printf("Failed to read UDP message: %v", err)
				continue
			}

			wg.Add(1)
			go func(src net.Addr, buffer []byte, n int) {
				defer wg.Done()

				message := strings.TrimSpace(string(buffer[:n]))
				handleSyslogMessage(message, rule)
			}(src, buffer, n)
		}

	default:
		log.Fatalf("Unknown mod: %s. Use 'tcp' or 'udp'.", config.Mod)
	}
}
