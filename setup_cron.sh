#!/bin/bash
# 現在のディレクトリを取得してCronに登録する内容を生成
CURRENT_DIR=$(pwd)
PYTHON_PATH="$CURRENT_DIR/venv/bin/python3"
SCRIPT_PATH="$CURRENT_DIR/virtual_bot.py"

echo "以下の設定をCronに登録してください："
echo ""
echo "30 15 * * 1-5 cd $CURRENT_DIR && $PYTHON_PATH $SCRIPT_PATH >> $CURRENT_DIR/virtual_bot.log 2>&1"
echo ""
echo "----------"
echo "【登録手順】"
echo "1. ターミナルで 'crontab -e' を実行"
echo "2. 'i' を押して入力モードにする"
echo "3. 上記の行を貼り付ける"
echo "4. 'Esc' を押して ':wq' を入力しEnter (保存して終了)"
echo "----------"
