import os
import ftplib
import pandas as pd
from datetime import datetime
import pytz
import schedule
import time
import shutil
import stat

#GitHubのテスト

#定数の定義
# INIファイルのフォルダ名
INI_FOLDER_NAME = 'Setting/01_FTP_File'

# 現在のファイルのディレクトリパスを取得
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# 設定ファイルの完全パス
INI_FOLDER_PATH = os.path.join(CURRENT_DIR, INI_FOLDER_NAME)

class FTPFileCopy:
    def __init__(self, parent, index):
        self._parent = parent
        self._index = index
        self._retry_count = 0
        self._retry_max = 3
        self._errlog_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'log/error/log.txt')
        self._norlog_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'log/normal/log.txt')

    def log_write(self, logtype, comment):
        jst = pytz.timezone('Asia/Tokyo')
        current_time = datetime.now(jst).strftime('%Y-%m-%d %H:%M:%S')
        log_message = f"{current_time} [{logtype}] {comment}"

        log_file_name = f"log_{datetime.now(jst).strftime('%Y%m%d_%H%M')}.txt"
        log_dir = os.path.dirname(self._errlog_file_path) if logtype == 'Err' else os.path.dirname(self._norlog_file_path)
        log_file_path = os.path.join(log_dir, log_file_name)

        
        with open(log_file_path, 'a', encoding='utf-8') as log_file:
            log_file.write(log_message + '\n')

    def read_ini_files(self, ini_folder_path):
        ini_files = [f for f in os.listdir(ini_folder_path) if f.endswith('.ini')]
        ftp_info_list = []
        for ini_file_name in ini_files:
            ini_file_path = os.path.join(ini_folder_path, ini_file_name)
            ini_contents = []
            if os.path.exists(ini_file_path):
                with open(ini_file_path, 'r', encoding='latin-1') as ini_file:
                    for line in ini_file:
                        ini_contents.append(line.strip().split('=', 1))
            else:
                print("ini file not found.")
                continue
            ini_df = pd.DataFrame(ini_contents, columns=['Key', 'Value'])
            
            # CopySakiの値に/dataを結合
            MachineNumber = ini_df.loc[ini_df['Key'] == 'MachineNumber']['Value'].values[0]
            copy_saki_base = ini_df.loc[ini_df['Key'] == 'CopySaki']['Value'].values[0]
        
            
            ftp_info = {
                'FTP_USER': ini_df.loc[ini_df['Key'] == 'UserName']['Value'].values[0],
                'FTP_PASS': ini_df.loc[ini_df['Key'] == 'Password']['Value'].values[0],
                'FTP_HOST': ini_df.loc[ini_df['Key'] == 'Ftphost']['Value'].values[0],
                'CopyMoto': ini_df.loc[ini_df['Key'] == 'CopyMoto']['Value'].values[0],
                'CopySaki': os.path.join(copy_saki_base, MachineNumber, 'data'),
                'Backup': os.path.join(copy_saki_base, MachineNumber, 'Backup'),
                'MachineNumber': MachineNumber,
                'Line_Count': ini_df.loc[ini_df['Key'] == 'Line_Count']['Value'].values[0],
                'CopyTemp': os.path.join(copy_saki_base, MachineNumber, 'CopyTemp')
            }
            ftp_info_list.append(ftp_info)
        return ftp_info_list

    def ftp_get_file_list(self, ftp, path):
        filelist = []
        datelist = []
        timelist = []
        sizelist = []
        try:
            ftp.cwd(path)
            files = ftp.nlst()
            for file in files:
                file_info = []
                try:
                    ftp.retrlines(f'LIST {file}', file_info.append)
                except ftplib.error_perm as e:
                    self.log_write('Err', f'ファイル情報取得権限エラー: {file}, 内容: {str(e)}')
                    continue
                except ftplib.error_temp as e:
                    self.log_write('Err', f'一時的なエラー: {file}, 内容: {str(e)}')
                    continue
                except ftplib.error_proto as e:
                    self.log_write('Err', f'プロトコルエラー: {file}, 内容: {str(e)}')
                    continue
                except ftplib.error_reply as e:
                    self.log_write('Err', f'サーバーからの予期しない応答: {file}, 内容: {str(e)}')
                    continue
                except ftplib.all_errors as e:
                    self.log_write('Err', f'ファイル情報取得エラー: {file}, 内容: {str(e)}')
                    continue
                except Exception as e:
                    self.log_write('Err', f'予期しないエラー: {file}, 内容: {str(e)}')
                    continue
                
                if file_info:
                    file_info = file_info[0]
                    if file.endswith(('.csv', '.CSV', '.txt', '.TXT', '.mat', '.MAT', '.pkl', '.PKL', '.zip', '.ZIP', '.mov', '.MOV', '.rdt', '.RDT')):
                        size = int(file_info.split()[4])
                        date_str = ' '.join(file_info.split()[5:8])
                        current_year = datetime.now().year
                        date = datetime.strptime(f"{current_year} {date_str}", '%Y %b %d %H:%M')
                        formatted_date = date.strftime('%Y-%m-%d %H:%M')
                        filelist.append(file)
                        datelist.append(formatted_date)
                        timelist.append(date.strftime('%H:%M:%S'))
                        sizelist.append(size)
            return filelist, datelist, timelist, sizelist
        except ftplib.error_perm as e:
            self.log_write('Err', f'パーミッションエラー: {str(e)}')
            return None, None, None, None
        except ftplib.error_temp as e:
            self.log_write('Err', f'一時的なエラー: {str(e)}')
            return None, None, None, None
        except ftplib.error_proto as e:
            self.log_write('Err', f'プロトコルエラー: {str(e)}')
            return None, None, None, None
        except ftplib.error_reply as e:
            self.log_write('Err', f'サーバーからの予期しない応答: {str(e)}')
            return None, None, None, None
        except ftplib.all_errors as e:
            self.log_write('Err', f'ftp_get_file_list_例外異常> 内容: {str(e)}')
            return None, None, None, None
        except Exception as e:
            self.log_write('Err', f'予期しないエラー: {str(e)}')
            return None, None, None, None

    def ftp_file_download(self, ftp, file, local_path, backup_path, temp_path, line_count_prm, machine_number):
        try:
            temp_file = os.path.join(temp_path, "temp_downloaded_file")
            with open(temp_file, 'wb') as f:
                ftp.retrbinary(f'RETR {file}', f.write)
            with open(temp_file, 'r') as f:
                lines = f.readlines()
                line_count = len(lines)
                if line_count < int(line_count_prm):
                    self.log_write('Err', f'行数の不足: {line_count}行')
                    return False
            with open(temp_file, 'r') as f:
                lines = [next(f) for _ in range(8)]
            new_file_path = self.file_read_timestamp_and_create_directory(lines, local_path, machine_number)         
            if os.path.exists(new_file_path):
                os.remove(new_file_path)
            
            # 名前変更
            os.rename(temp_file, new_file_path)

            # 書き込み権限と削除権限（読み取り権限も含む）を付与
            # 所有者に読み書き実行権限、グループとその他に読み込み権限を付与
            permissions = stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IROTH
            os.chmod(new_file_path, permissions)

            # バックアップの作成
            filename = os.path.basename(new_file_path)
            timestamp_part = filename.split('_')[1][:8]
            backup_path = os.path.join(backup_path, timestamp_part)
            
            if not os.path.exists(backup_path):
                os.makedirs(backup_path)
            backup_file_path = os.path.join(backup_path, os.path.basename(new_file_path))
            try:
                shutil.copy2(new_file_path, backup_file_path)
                self.log_write('Nor', f'バックアップ作成成功: {backup_file_path}')
            except Exception as e:
                self.log_write('Err', f'バックアップ作成失敗: {str(e)}')
                # バックアップ失敗してもメイン処理は続行
            
            return True
        except ftplib.error_perm as e:
            self.log_write('Err', f'パーミッションエラー: {str(e)}')
            return False
        except ftplib.all_errors as e:
            self.log_write('Err', f'ftp_file_download_例外異常> 内容: {str(e)}')
            return False
        except Exception as e:
            self.log_write('Err', f'予期しないエラー: {str(e)}')
            return False

    def ftp_file_delete(self, ftp, remote_file, local_path, machine_number):
        try:
            lines = []
            ftp.retrlines(f'RETR {remote_file}', lines.append)
            chk_file_path = self.file_read_timestamp_and_create_directory(lines, local_path, machine_number)
            if os.path.exists(chk_file_path):
                try:
                    ftp.delete(remote_file)
                    self.log_write('Nor', f'FTPファイル削除成功: {remote_file}')
                    return True
                except ftplib.error_perm as e:
                    self.log_write('Err', f'FTPファイル削除権限エラー: {str(e)}')
                    return False
                except ftplib.all_errors as e:
                    self.log_write('Err', f'FTPファイル削除エラー: {str(e)}')
                    return False
            else:
                self.log_write('Err', f'ローカルファイルが存在しません: {chk_file_path}')
                return False
        except ftplib.all_errors as e:
            self.log_write('Err', f'FTPファイル読み込みエラー: {str(e)}')
            return False
        except Exception as e:
            self.log_write('Err', f'予期しないエラー: {str(e)}')
            return False

    def retry_check(self, ret):
        if not ret:
            self._retry_count += 1
            if self._retry_count >= self._retry_max:
                self.log_write('Err', 'リトライカウントオーバー')
                self._retry_count = 0
                return True
            else:
                return False
        else:
            self._retry_count = 0
            return True

    def file_read_timestamp_and_create_directory(self, lines, local_path, machine_number):
        try:
         
            # タイムスタンプを解析
            timestamp_str = lines[7].split(',')[2].strip()
            self.log_write('Nor', f'抽出されたタイムスタンプ: {timestamp_str}')
            
            base_time = timestamp_str.split('_')[0]  # 'yyyy/mm/dd hh:mm'部分を取得
             
            timestamp = datetime.strptime(base_time, '%Y/%m/%d %H:%M')
            seconds = timestamp_str.split('_')[1].replace('s', '')  # '34s'から's'を除去して'34'を取得
            
            
            
            # ディレクトリパスを作成
            year_month_day = timestamp.strftime('%Y%m%d')
            # YYYYMMDDHHMMSSの形式に変換
            formatted_timestamp = f"{timestamp.strftime('%Y%m%d%H%M')}{seconds}"
            
            # ディレクトリ構造を作成
            dir_path = os.path.join(local_path, year_month_day)
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
                
            # 新しいファイル名を生成
            file_name = f"{machine_number}_{formatted_timestamp}.csv"
            new_file_path = os.path.join(dir_path, file_name)
            
            return new_file_path
                
        except Exception as e:
            self.log_write('Err', f'タイムスタンプ処理エラー: {str(e)}')
            return None

    def thread_execute(self):
        pass

    def thread_dispose(self):
        pass

def job():  # インデントを修正
    parent = None
    index = 1
    ftp_copy = FTPFileCopy(parent, index)
    ftp_info_list = ftp_copy.read_ini_files(INI_FOLDER_PATH)
    
    for ftp_info in ftp_info_list:
        ftp_copy = FTPFileCopy(parent, index)
        retry_count = 0
        max_retries = 3
        connected = False
        
        while retry_count < max_retries and not connected:
            ftp = ftplib.FTP(timeout=600)
            try:
                ftp.connect(ftp_info['FTP_HOST'], ftp_info.get('FTP_PORT', 21))
                ftp.login(ftp_info['FTP_USER'], ftp_info['FTP_PASS'])
                ftp.set_pasv(True)  # パッシブモードを有効にする
                ftp_copy.log_write('Nor', f'FTP接続およびログイン成功: {ftp_info["FTP_HOST"]}')
                connected = True
            except ftplib.error_perm as e:
                retry_count += 1
                ftp_copy.log_write('Err', f'FTP接続権限エラー({retry_count}/{max_retries}): {str(e)}')
                if retry_count >= max_retries:
                    break
                time.sleep(5)  # リトライ前に少し待機
            except ftplib.all_errors as e:
                retry_count += 1
                ftp_copy.log_write('Err', f'FTP接続エラー({retry_count}/{max_retries}): {str(e)}')
                if retry_count >= max_retries:
                    break
                time.sleep(5)  # リトライ前に少し待機
            except Exception as e:
                retry_count += 1
                ftp_copy.log_write('Err', f'予期しないエラー({retry_count}/{max_retries}): {str(e)}')
                if retry_count >= max_retries:
                    break
                time.sleep(5)  # リトライ前に少し待機
        
        if not connected:
            ftp_copy.log_write('Err', f'リトライ回数({max_retries}回)を超えました。FTP接続を諦めます: {ftp_info["FTP_HOST"]}')
            continue

        try:
            filelist, datelist, timelist, sizelist = ftp_copy.ftp_get_file_list(ftp, ftp_info['CopyMoto'])
            
            
            if filelist:
                for file in filelist:
                    folder_path = os.path.join(CURRENT_DIR, ftp_info['CopySaki'])
                    backup_path = os.path.join(CURRENT_DIR, ftp_info['Backup'])
                    temp_path = os.path.join(CURRENT_DIR, ftp_info['CopyTemp'])
                    if not os.path.exists(folder_path):
                        os.makedirs(folder_path)
                    if not os.path.exists(temp_path):
                        os.makedirs(temp_path)
                    if ftp_copy.ftp_file_download(ftp, file, folder_path, backup_path, temp_path, ftp_info['Line_Count'], ftp_info['MachineNumber']):
                        ftp_copy.log_write('Nor', 'FTPファイルダウンロード成功') 
                        # ファイル削除
                        #ftp_copy.ftp_file_delete(ftp, file, folder_path, ftp_info['MachineNumber'])
        except ftplib.error_perm as e:
            ftp_copy.log_write('Err', f'ファイルリスト取得権限エラー: {str(e)}')
        except ftplib.all_errors as e:
            ftp_copy.log_write('Err', f'ファイルリスト取得エラー: {str(e)}')
        except Exception as e:
            ftp_copy.log_write('Err', f'予期しないエラー: {str(e)}')
        finally:
            ftp.quit()
            
    ftp_copy.log_write('Nor', 'すべてのFTPファイルコピー処理が完了しました')

if __name__ == "__main__":
    job()
    schedule.every(10).minutes.do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)