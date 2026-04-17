package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	cli "github.com/jawher/mow.cli"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Config 数据库配置结构
type Config struct {
	Host           string `json:"host"`
	Port           int    `json:"port"`
	User           string `json:"user"`
	Password       string `json:"password"`
	Database       string `json:"database"`
	TargetDatabase string `json:"target_database"` // 新增：目标数据库名
}

// BackupConfig 备份配置
type BackupConfig struct {
	OutputDir    string
	OutputFile   string
	Tables       []string
	Where        string
	NoData       bool
	NoCreateInfo bool
	SkipComments bool
}

// RestoreConfig 恢复配置
type RestoreConfig struct {
	SQLFile   string
	BatchSize int
	Force     bool
}

// 全局配置
var dbConfig Config

func main() {
	app := cli.App("mysql-tool", "MySQL 备份和恢复工具")

	// 全局选项：配置文件路径
	configFile := app.StringOpt("c config", "config.json", "配置文件路径")

	// 加载配置
	app.Before = func() {
		loadConfig(*configFile)
	}

	// 备份命令
	app.Command("backup", "备份数据库", func(cmd *cli.Cmd) {
		outputDir := cmd.StringOpt("o output", "./backups", "输出目录")
		outputFile := cmd.StringOpt("f file", "", "输出文件名（默认自动生成）")
		tables := cmd.StringsOpt("t tables", nil, "指定要备份的表（可选，不指定则备份所有）")
		noData := cmd.BoolOpt("no-data", false, "只导出结构，不导出数据")
		noCreate := cmd.BoolOpt("no-create-info", false, "不导出CREATE TABLE语句")

		cmd.Action = func() {
			backupConfig := BackupConfig{
				OutputDir:    *outputDir,
				OutputFile:   *outputFile,
				Tables:       *tables,
				NoData:       *noData,
				NoCreateInfo: *noCreate,
			}
			backup(backupConfig)
		}
	})

	// 恢复命令
	app.Command("restore", "恢复数据库", func(cmd *cli.Cmd) {
		sqlFile := cmd.StringArg("FILE", "", "要恢复的SQL文件路径")
		batchSize := cmd.IntOpt("batch-size", 1000, "批量执行的SQL语句数量")
		force := cmd.BoolOpt("f force", false, "强制恢复（忽略错误）")

		cmd.Action = func() {
			if *sqlFile == "" {
				fmt.Println("错误：请指定要恢复的SQL文件")
				os.Exit(1)
			}
			restoreConfig := RestoreConfig{
				SQLFile:   *sqlFile,
				BatchSize: *batchSize,
				Force:     *force,
			}
			restore(restoreConfig)
		}
	})

	app.Run(os.Args)
}

// 加载配置文件
func loadConfig(configFile string) {
	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		fmt.Printf("读取配置文件失败: %v\n", err)
		os.Exit(1)
	}

	err = json.Unmarshal(data, &dbConfig)
	if err != nil {
		fmt.Printf("解析配置文件失败: %v\n", err)
		os.Exit(1)
	}

	// 验证配置
	if dbConfig.Host == "" || dbConfig.User == "" {
		fmt.Println("配置文件缺少必要字段: host, user")
		os.Exit(1)
	}

	if dbConfig.Port == 0 {
		dbConfig.Port = 3306 // 默认端口
	}
}

// 构建MySQL连接字符串
func buildDSN(database string) string {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		dbConfig.User,
		dbConfig.Password,
		dbConfig.Host,
		dbConfig.Port,
	)

	if database != "" {
		dsn += database
	}

	dsn += "?charset=utf8mb4&parseTime=True&loc=Local"
	return dsn
}

// 备份功能
func backup(config BackupConfig) {
	// 创建输出目录
	if err := os.MkdirAll(config.OutputDir, 0755); err != nil {
		fmt.Printf("创建输出目录失败: %v\n", err)
		os.Exit(1)
	}

	// 确定输出文件名
	var outputFilePath string
	if config.OutputFile != "" {
		outputFilePath = filepath.Join(config.OutputDir, config.OutputFile)
	} else {
		timestamp := time.Now().Format("20060102_150405")
		dbName := dbConfig.Database
		if dbName == "" {
			dbName = "mysql"
		}
		outputFilePath = filepath.Join(config.OutputDir, fmt.Sprintf("%s_%s.sql", dbName, timestamp))
	}

	// 构建mysqldump命令
	args := []string{
		fmt.Sprintf("-u%s", dbConfig.User),
		fmt.Sprintf("-p%s", dbConfig.Password),
		"-h", dbConfig.Host,
		"-P", fmt.Sprintf("%d", dbConfig.Port),
		"--single-transaction",
		"--routines",
		"--triggers",
		"--no-create-db",
	}

	// 添加忽略不存在的表
	// 注意：表名需要全部小写，因为 MySQL 在不区分大小写的系统上会有问题
	args = append(args, fmt.Sprintf("--ignore-table=%s.databasechangelog", dbConfig.Database))
	args = append(args, fmt.Sprintf("--ignore-table=%s.DATABASECHANGELOG", dbConfig.Database))

	// 可选：添加其他参数
	if config.NoData {
		args = append(args, "--no-data")
	}
	if config.NoCreateInfo {
		args = append(args, "--no-create-info")
	}
	if config.SkipComments {
		args = append(args, "--skip-comments")
	}

	// 添加数据库名
	args = append(args, dbConfig.Database)

	// 如果指定了表，则使用指定的表
	if len(config.Tables) > 0 {
		args = append(args, config.Tables...)
	}

	// 执行mysqldump
	fmt.Printf("开始备份数据库: %s\n", dbConfig.Database)
	fmt.Printf("输出文件: %s\n", outputFilePath)

	// 隐藏密码显示
	displayArgs := make([]string, len(args))
	copy(displayArgs, args)
	for i, arg := range displayArgs {
		if strings.HasPrefix(arg, "-p") && len(arg) > 2 {
			displayArgs[i] = "-p***"
		}
	}
	fmt.Printf("执行命令: mysqldump %s\n", strings.Join(displayArgs, " "))

	cmd := exec.Command("./mysqldump", args...)

	// 创建输出文件
	file, err := os.Create(outputFilePath)
	if err != nil {
		fmt.Printf("创建输出文件失败: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	// 设置输出
	cmd.Stdout = file
	cmd.Stderr = os.Stderr

	// 执行命令
	err = cmd.Run()
	if err != nil {
		fmt.Printf("备份失败: %v\n", err)
		os.Exit(1)
	}

	// 获取文件大小
	fileInfo, _ := file.Stat()
	fileSize := float64(fileInfo.Size()) / 1024 / 1024

	fmt.Printf("备份完成！\n")
	fmt.Printf("文件大小: %.2f MB\n", fileSize)
	fmt.Printf("保存路径: %s\n", outputFilePath)
}

// 恢复功能 - 使用 source 方式
func restore(config RestoreConfig) {
	// 检查SQL文件是否存在
	if _, err := os.Stat(config.SQLFile); os.IsNotExist(err) {
		fmt.Printf("SQL文件不存在: %s\n", config.SQLFile)
		os.Exit(1)
	}

	// 确定目标数据库名
	targetDB := dbConfig.TargetDatabase
	if targetDB == "" {
		targetDB = dbConfig.Database
	}

	fmt.Printf("开始恢复数据库\n")
	fmt.Printf("源文件: %s\n", config.SQLFile)
	fmt.Printf("目标数据库: %s\n", targetDB)
	fmt.Printf("目标主机: %s:%d\n", dbConfig.Host, dbConfig.Port)

	// 询问确认删除数据库
	fmt.Printf("⚠️  警告：即将删除数据库 '%s' 并重建！\n", targetDB)
	fmt.Print("确认继续？(y/n): ")
	var response string
	fmt.Scanln(&response)
	if strings.ToLower(response) != "y" && strings.ToLower(response) != "yes" {
		fmt.Println("操作已取消")
		os.Exit(0)
	}

	// 第一步：连接并重建数据库
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&parseTime=True&loc=Local",
		dbConfig.User,
		dbConfig.Password,
		dbConfig.Host,
		dbConfig.Port,
	)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Printf("连接数据库失败: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// 删除并重建数据库
	fmt.Printf("🗑️  删除数据库: %s\n", targetDB)
	_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", targetDB))
	if err != nil {
		fmt.Printf("删除数据库失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("📦 创建数据库: %s\n", targetDB)
	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin", targetDB))
	if err != nil {
		// 回退到 utf8mb4_unicode_ci
		_, err = db.Exec(fmt.Sprintf("CREATE DATABASE `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", targetDB))
		if err != nil {
			fmt.Printf("创建数据库失败: %v\n", err)
			os.Exit(1)
		}
	}
	fmt.Printf("✅ 数据库 %s 已创建\n", targetDB)

	// 第二步：使用 mysql 客户端执行 source
	fmt.Println("\n📥 开始导入数据...")
	err = executeWithRedirect(targetDB, config.SQLFile)
	if err != nil {
		fmt.Printf("导入失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n🎉 恢复完成！")
}

// 使用重定向方式执行 SQL 文件（本地文件直接传给远程 MySQL）
func executeWithRedirect(databaseName, sqlFile string) error {
	// 使用当前目录的 mysql.exe
	mysqlPath := ".\\mysql.exe"

	// 构建 mysql 命令参数
	args := []string{
		fmt.Sprintf("-u%s", dbConfig.User),
		fmt.Sprintf("-p%s", dbConfig.Password),
		fmt.Sprintf("-h%s", dbConfig.Host),
		fmt.Sprintf("-P%d", dbConfig.Port),
		"--default-character-set=utf8mb4",
		databaseName,
	}

	// 显示命令（隐藏密码）
	displayArgs := make([]string, len(args))
	copy(displayArgs, args)
	for i, arg := range displayArgs {
		if strings.HasPrefix(arg, "-p") && len(arg) > 2 {
			displayArgs[i] = "-p***"
		}
	}
	fmt.Printf("执行命令: %s %s < %s\n", mysqlPath, strings.Join(displayArgs, " "), sqlFile)

	// 检查 mysql.exe 是否存在
	if _, err := os.Stat(mysqlPath); os.IsNotExist(err) {
		return fmt.Errorf("mysql.exe 不存在于当前目录: %s", mysqlPath)
	}

	// 检查 SQL 文件是否存在
	if _, err := os.Stat(sqlFile); os.IsNotExist(err) {
		return fmt.Errorf("SQL 文件不存在: %s", sqlFile)
	}

	// 打开本地 SQL 文件
	file, err := os.Open(sqlFile)
	if err != nil {
		return fmt.Errorf("打开 SQL 文件失败: %v", err)
	}
	defer file.Close()

	// 执行命令，将文件内容通过 stdin 传入
	cmd := exec.Command(mysqlPath, args...)
	cmd.Stdin = file
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	startTime := time.Now()
	err = cmd.Run()
	duration := time.Since(startTime)

	if err != nil {
		return fmt.Errorf("执行 mysql 命令失败: %v (耗时: %v)", err, duration)
	}

	fmt.Printf("✅ 导入完成，耗时: %v\n", duration)
	return nil
}

// 获取数据库列表（辅助功能）
func getDatabaseList() ([]string, error) {
	dsn := buildDSN("")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query("SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return nil, err
		}
		// 跳过系统数据库
		if dbName != "information_schema" &&
			dbName != "performance_schema" &&
			dbName != "mysql" &&
			dbName != "sys" {
			databases = append(databases, dbName)
		}
	}

	return databases, nil
}
