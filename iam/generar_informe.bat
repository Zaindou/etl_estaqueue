@echo off
cd ..
call env\Scripts\activate
python main.py
call env\Scripts\deactivate
pause