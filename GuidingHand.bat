@echo off
chcp 65001 >nul
cd /d "%~dp0"
:menu
cls
echo ==========================================================
echo   GUIDING HAND - Big Data (FIAP GS 2026) - Menu
echo ==========================================================
echo   0. Verificar se o computador esta pronto
echo   1. Validar as fontes de dados (gera relatorio)
echo   2. Iniciar o Airflow (Docker)
echo   3. Executar a coleta agora
echo   4. Preparar lote para o Databricks
echo   5. Rodar os testes automatizados
echo   6. Verificar segredos antes de publicar
echo   7. Abrir a tela do MVP
echo   8. Gerar o relatorio PDF
echo   G. Publicar no GitHub
echo   9. Parar o Airflow
echo   S. Sair
echo ==========================================================
set "op="
set /p "op=Escolha: "
if /i "%op%"=="S" exit /b
set "arq="
if "%op%"=="0" set "arq=00_verificar_ambiente.ps1"
if "%op%"=="1" set "arq=01_validar_fontes.ps1"
if "%op%"=="2" set "arq=02_iniciar_airflow.ps1"
if "%op%"=="3" set "arq=03_executar_coleta.ps1"
if "%op%"=="4" set "arq=04_preparar_lote.ps1"
if "%op%"=="5" set "arq=05_rodar_testes.ps1"
if "%op%"=="6" set "arq=06_verificar_segredos.ps1"
if "%op%"=="7" set "arq=07_abrir_tela_mvp.ps1"
if "%op%"=="8" set "arq=08_gerar_relatorio.ps1"
if /i "%op%"=="G" set "arq=09_publicar_github.ps1"
if "%op%"=="9" set "arq=99_parar_airflow.ps1"
if not defined arq goto menu
powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\windows\%arq%"
echo.
pause
goto menu
