@echo off
setlocal EnableDelayedExpansion

rem Simple menu to run common project commands.
rem Uses the repo-local Python 3.12 venv at .\.venv.

set ROOT=%~dp0
set ROOT=%ROOT:~0,-1%
set PY="%ROOT%\.venv\Scripts\python"
set PSHELL=powershell -NoLogo -NoProfile

:menu
echo.
echo ===== finviz_long_trader menu =====
echo [1] Start brains service
echo [2] Force EOD liquidation (run_eod_now)
echo [10] Force EOD liquidation after-hours (limit orders)
echo [3] Run tests (pytest)
echo [4] Refresh dependencies (pip -r requirements.txt)
echo [5] Intraday high-of-day check (intraday_high)
echo [6] Summarize a PnL log (pnl_summary --file ^<path^>)
echo [7] Alpaca diag (clock/account + test order)
echo [8] Alpaca fills for today
echo [9] Alpaca clear-all (cancel orders, close positions)
echo [0] Exit
echo ===================================
set /p choice=Select an option: 

if "%choice%"=="1" goto brains
if "%choice%"=="2" goto eod
if "%choice%"=="10" goto eod_after
if "%choice%"=="3" goto tests
if "%choice%"=="4" goto deps
if "%choice%"=="5" goto intraday
if "%choice%"=="6" goto pnl
if "%choice%"=="7" goto diag
if "%choice%"=="8" goto fills
if "%choice%"=="9" goto clearall
if "%choice%"=="0" goto end
echo Invalid choice.& goto menu

:brains
cd /d "%ROOT%"
%PY% -m src.brain.brain_service
goto aftercmd

:eod
cd /d "%ROOT%"
%PY% -m src.brain.run_eod_now
goto aftercmd

:eod_after
cd /d "%ROOT%"
%PY% -m src.brain.run_eod_now --after-hours
goto aftercmd

:tests
cd /d "%ROOT%"
%PY% -m pytest
goto aftercmd

:deps
cd /d "%ROOT%"
%PY% -m pip install -r requirements.txt
goto aftercmd

:intraday
cd /d "%ROOT%"
set /p syms=Symbols (comma-separated, blank for default): 
if "%syms%"=="" (
  %PY% -m src.tools.intraday_high
) else (
  %PY% -m src.tools.intraday_high --symbols %syms%
)
goto aftercmd

:pnl
cd /d "%ROOT%"
set /p pnllog=PnL log path (e.g., data\pnl-2025-12-12.log): 
if "%pnllog%"=="" (
  echo No file provided. Returning to menu.
  goto menu
)
%PY% -m src.tools.pnl_summary --file "%pnllog%"
goto aftercmd

:diag
cd /d "%ROOT%"
%PY% -m src.tools.alpaca_diag
goto aftercmd

:fills
cd /d "%ROOT%"
%PY% -m src.tools.alpaca_fills
goto aftercmd

:clearall
cd /d "%ROOT%"
%PY% -m src.tools.alpaca_clear_all
goto aftercmd

:aftercmd
echo.
echo Press Enter to return to the menu, or close this window to exit.
pause >nul
goto menu

:end
endlocal
