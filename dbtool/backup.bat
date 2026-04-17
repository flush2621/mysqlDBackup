@echo off
chcp 65001 > nul
setlocal enabledelayedexpansion

:: 获取脚本所在目录
set "TOOL_DIR=%~dp0"
set "TOOL_DIR=%TOOL_DIR:~0,-1%"

:: 切换到工具目录
cd /d "%TOOL_DIR%"

:: 将当前目录加入 PATH，确保 DLL 能被找到
set "PATH=%CD%;%PATH%"

:: 创建日志目录
if not exist "%TOOL_DIR%\logs" mkdir "%TOOL_DIR%\logs"
set "LOG_FILE=%TOOL_DIR%\logs\backup_%date:~0,4%%date:~5,2%%date:~8,2%.log"

:: 记录执行信息
echo ======================================== >> "%LOG_FILE%"
echo 备份开始时间: %date% %time% >> "%LOG_FILE%"
echo 当前工作目录: %CD% >> "%LOG_FILE%"
echo PATH: %PATH% >> "%LOG_FILE%"

:: 检查依赖文件是否存在
if exist "%CD%\mysqlBackup.exe" (
    echo mysqlBackup.exe 存在 >> "%LOG_FILE%"
) else (
    echo 错误: mysqlBackup.exe 不存在于 %CD% >> "%LOG_FILE%"
)

:: 执行备份命令
"%CD%\mysqlBackup.exe" backup

:: 记录退出码
echo 退出码: %errorlevel% >> "%LOG_FILE%"

if %errorlevel% equ 0 (
    echo 备份成功 >> "%LOG_FILE%"
) else (
    echo 备份失败，错误码: %errorlevel% >> "%LOG_FILE%"
)

echo 备份结束时间: %date% %time% >> "%LOG_FILE%"
echo ======================================== >> "%LOG_FILE%"

exit /b %errorlevel%