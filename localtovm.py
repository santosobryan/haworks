import paramiko
import getpass
import os
import time
from pathlib import Path
import io

def upload_via_jump_server_continuous():
    """Continuous SFTP upload program with automatic dos2unix conversion"""
    
    print("=== CONTINUOUS SFTP UPLOAD VIA JUMP SERVER ===")
    print("This program automatically converts text files from DOS to Unix line endings.\n")
    
    connection_cache = {
        'jump_password': None,
        'target_password': None,
        'jump_ssh': None,
        'target_ssh': None,
        'sftp': None,
        'connected': False,
        'text_extensions': {'.txt', '.sh', '.py', '.pl', '.conf', '.cfg', '.ini', '.log', 
                          '.xml', '.json', '.yaml', '.yml', '.properties', '.sql', '.js', 
                          '.css', '.html', '.htm', '.md', '.csv', '.tsv', '.bat', '.ps1'}
    }
    
    def is_text_file(file_path):
        """Check if file should be converted based on extension"""
        return file_path.suffix.lower() in connection_cache['text_extensions']
    
    def is_binary_file(file_path):
        """Quick binary file detection to avoid converting binary files"""
        try:
            with open(file_path, 'rb') as f:
                chunk = f.read(1024)
                return b'\0' in chunk
        except:
            return False
    
    def convert_dos2unix_stream(local_file_path):
        """Convert DOS line endings to Unix during upload"""
        try:
            print(f"    → Converting DOS to Unix line endings...")
            with open(local_file_path, 'rb') as f:
                content = f.read()
            try:
                text_content = content.decode('utf-8')
            except UnicodeDecodeError:
                try:
                    text_content = content.decode('latin-1')
                    print(f"      (using latin-1 encoding)")
                except UnicodeDecodeError:
                    print(f"      (cannot decode - uploading as binary)")
                    return None
            
            original_lines = text_content.count('\r\n')
            unix_content = text_content.replace('\r\n', '\n')
            
            unix_bytes = unix_content.encode('utf-8')
            unix_file_obj = io.BytesIO(unix_bytes)
            
            if original_lines > 0:
                print(f"      ✓ Converted {original_lines} DOS line endings")
            else:
                print(f"      → Already Unix format")
            
            return unix_file_obj
            
        except Exception as e:
            print(f"      ✗ Conversion failed: {e}")
            return None
    
    def get_connection_details():
        """Get connection details from user"""
        while True:
            try:
                print("=== CONNECTION SETUP ===")
                
                jump_host = input("Jump server hostname (default: sshgateway): ").strip() or "sshgateway"
                jump_user = input("Jump server username (corpID): ").strip()
                target_host = input("Target VM hostname (default: cmspqlvmctst11j): ").strip() or "cmspqlvmctst11j"
                target_user = input("Target VM username (default: cmscorpadm): ").strip() or "cmscorpadm"
                
                return jump_host, jump_user, target_host, target_user
                
            except KeyboardInterrupt:
                print("\nConnection setup interrupted. Trying again...")
                continue
            except Exception as e:
                print(f"Error in connection setup: {e}. Trying again...")
                continue
    
    def safe_connect(jump_host, jump_user, target_host, target_user):
        """Safely establish connection with continuous retry on failure"""
        connected = False

        while not connected:
            try:
                if connection_cache['jump_password'] is None:
                    try:
                        connection_cache['jump_password'] = getpass.getpass(f"Password for {jump_user}@{jump_host} (jump server): ")
                    except KeyboardInterrupt:
                        print("\nPassword entry interrupted. Trying again...")
                        continue
                
                if connection_cache['target_password'] is None:
                    try:
                        connection_cache['target_password'] = getpass.getpass(f"Password for {target_user}@{target_host} (target VM): ")
                    except KeyboardInterrupt:
                        print("\nPassword entry interrupted. Trying again...")
                        continue

                print(f"Connecting to jump server: {jump_host}")
                jump_ssh = paramiko.SSHClient()
                jump_ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                jump_ssh.connect(jump_host, username=jump_user, password=connection_cache['jump_password'], timeout=30)
                print("✓ Connected to jump server")
                
                print(f"Creating tunnel to target VM: {target_host}")
                jump_transport = jump_ssh.get_transport()
                dest_addr = (target_host, 22)
                local_addr = ('127.0.0.1', 22)
                channel = jump_transport.open_channel("direct-tcpip", dest_addr, local_addr)
                print("✓ Tunnel created")
                
                print(f"Connecting to target VM through tunnel...")
                target_ssh = paramiko.SSHClient()
                target_ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                target_ssh.connect(target_host, username=target_user, password=connection_cache['target_password'], sock=channel, timeout=30)
                print("✓ Connected to target VM")
                
                sftp = target_ssh.open_sftp()
                print("✓ SFTP session established")
                connected = True
                
                connection_cache.update({
                    'jump_ssh': jump_ssh,
                    'target_ssh': target_ssh,
                    'sftp': sftp,
                    'connected': True
                })
                
                return True
                
            except paramiko.AuthenticationException as e:
                print(f"✗ Authentication failed: {e}")
                print("Clearing stored passwords...")
                connection_cache['jump_password'] = None
                connection_cache['target_password'] = None
                if not connected:
                    print(f"Retrying authentication...")
                    time.sleep(2)
                continue
                
            except KeyboardInterrupt:
                print("\nConnection interrupted. Trying again...")
                continue
                
            except Exception as e:
                print(f"✗ Connection failed: {e}")
                if not connected:
                    print(f"Retrying connection...")
                    time.sleep(2)
                continue
        return False
    
    def safe_disconnect():
        """Safely close all connections"""
        try:
            if connection_cache['sftp']:
                connection_cache['sftp'].close()
        except:
            pass
        
        try:
            if connection_cache['target_ssh']:
                connection_cache['target_ssh'].close()
        except:
            pass
        
        try:
            if connection_cache['jump_ssh']:
                connection_cache['jump_ssh'].close()
        except:
            pass
        
        connection_cache.update({
            'jump_ssh': None,
            'target_ssh': None,
            'sftp': None,
            'connected': False
        })
    
    def get_local_path():
        """Get and validate local path - keep trying until valid"""
        while True:
            try:
                print("\n=== LOCAL PATH SELECTION ===")
                default_path = r""
                
                try:
                    user_input = input(f"Enter local path: ").strip()
                except KeyboardInterrupt:
                    print("\nInput interrupted. Trying again...")
                    continue
                
                if not user_input:
                    if default_path:
                        local_path = Path(default_path)
                    else:
                        print("Please enter a local path.")
                        continue
                else:
                    local_path = Path(user_input)
                
                if local_path.exists():
                    print(f"✓ Local path exists: {local_path}")
                    return local_path
                else:
                    print(f"✗ Local path does not exist: {local_path}")
                    print("Please try again with a valid path.")
                    continue
                        
            except Exception as e:
                print(f"✗ Error with local path: {e}")
                print("Please try again.")
                continue
    
    def get_target_path():
        """Get and validate target path - keep trying until valid"""
        while True:
            try:
                target_ssh = connection_cache['target_ssh']

                try:
                    stdin, stdout, stderr = target_ssh.exec_command('pwd')
                    current_dir = stdout.read().decode().strip()
                    
                    stdin, stdout, stderr = target_ssh.exec_command('echo $HOME')
                    home_dir = stdout.read().decode().strip()
                    
                    print(f"\n=== TARGET PATH SELECTION ===")
                    print(f"Current directory: {current_dir}")
                    print(f"Home directory: {home_dir}")
                    
                except Exception as e:
                    print(f"✗ Error getting directory info: {e}")
                    current_dir = "/tmp"
                    home_dir = "/tmp"
                    print(f"Using fallback directories: {current_dir}")
                
                try:
                    print(f"\nOptions:")
                    print(f"  - Press ENTER for current directory ({current_dir})")
                    print(f"  - Type ~ for home directory ({home_dir})")
                    print(f"  - Type a full path (e.g., /tmp)")
                    print(f"  - Type 'list' to see current directory contents")
                    
                    user_input = input(f"Enter target path: ").strip()
                    
                except KeyboardInterrupt:
                    print("\nInput interrupted. Trying again...")
                    continue
                
                if user_input == "":
                    target_path = current_dir
                elif user_input.lower() == "list":
                    try:
                        stdin, stdout, stderr = target_ssh.exec_command(f'ls -la "{current_dir}"')
                        listing = stdout.read().decode()
                        print(f"\nContents of {current_dir}:")
                        print(listing)
                    except Exception as e:
                        print(f"Error listing directory: {e}")
                    continue
                elif user_input == "~":
                    target_path = home_dir
                else:
                    if user_input.startswith('~'):
                        target_path = user_input.replace('~', home_dir)
                    else:
                        target_path = user_input
                
                try:
                    stdin, stdout, stderr = target_ssh.exec_command(f'test -d "{target_path}" && echo "EXISTS" || echo "NOT_EXISTS"')
                    exists_check = stdout.read().decode().strip()
                    
                    if exists_check == "EXISTS":
                        stdin, stdout, stderr = target_ssh.exec_command(f'test -w "{target_path}" && echo "WRITABLE" || echo "NOT_WRITABLE"')
                        writable_check = stdout.read().decode().strip()
                        
                        if writable_check == "WRITABLE":
                            print(f"✓ Path is valid and writable: {target_path}")
                            return target_path
                        else:
                            print(f"✗ Path exists but is not writable: {target_path}")
                            print("Please choose a different path.")
                            continue
                    else:
                        print(f"✗ Path does not exist: {target_path}")
                        try:
                            create_choice = input("Try creating this directory? (y/n): ").strip().lower()
                        except KeyboardInterrupt:
                            print("\nInput interrupted. Trying again...")
                            continue
                            
                        if create_choice in ['y', 'yes']:
                            try:
                                stdin, stdout, stderr = target_ssh.exec_command(f'mkdir -p "{target_path}"')
                                error = stderr.read().decode().strip()
                                if not error:
                                    print(f"✓ Successfully created directory: {target_path}")
                                    return target_path
                                else:
                                    print(f"✗ Failed to create directory: {error}")
                                    print("Please try a different path.")
                                    continue
                            except Exception as e:
                                print(f"✗ Error creating directory: {e}")
                                print("Please try a different path.")
                                continue
                        else:
                            print("Please choose an existing directory.")
                            continue
                
                except Exception as e:
                    print(f"✗ Error validating path: {e}")
                    print("Please try a different path.")
                    continue
                
            except Exception as e:
                print(f"✗ Error in path selection: {e}")
                print("Trying again...")
                time.sleep(1)
                continue
    
    def perform_upload(local_path, target_base_path):
        """Perform upload with automatic dos2unix conversion"""
        try:
            sftp = connection_cache['sftp']
            target_dir = f"{target_base_path}/{local_path.name}"
            
            print(f"\nCreating target directory: {target_dir}")
            try:
                sftp.mkdir(target_dir)
                print(f"✓ Created directory: {target_dir}")
            except Exception as e:
                if "File exists" in str(e):
                    print(f"✓ Directory already exists: {target_dir}")
                else:
                    print(f"✗ Failed to create directory: {e}")
                    return False
            
            def upload_recursive(local_dir, remote_dir):
                uploaded_count = 0
                failed_count = 0
                converted_count = 0
                
                try:
                    for item in local_dir.iterdir():
                        try:
                            local_item = local_dir / item.name
                            remote_item = f"{remote_dir}/{item.name}"
                            
                            if item.is_file():
                                print(f"Uploading: {item.name}")
                            
                                should_convert = is_text_file(item) and not is_binary_file(item)
                                
                                if should_convert:
                                    print(f"  → Text file detected, applying dos2unix conversion")
                                    converted_file = convert_dos2unix_stream(local_item)
                                    
                                    if converted_file:
                                        sftp.putfo(converted_file, remote_item)
                                        converted_file.close()
                                        print(f"  ✓ Uploaded with conversion: {item.name}")
                                        converted_count += 1
                                    else:
                                        print(f"  → Conversion failed, uploading original file")
                                        sftp.put(str(local_item), remote_item)
                                        print(f"  ✓ Uploaded (original): {item.name}")
                                else:
                                    sftp.put(str(local_item), remote_item)
                                    print(f"  ✓ Uploaded: {item.name}")
                                
                                uploaded_count += 1
                                
                            elif item.is_dir():
                                print(f"Creating subdirectory: {item.name}")
                                try:
                                    sftp.mkdir(remote_item)
                                except Exception as e:
                                    if "File exists" not in str(e):
                                        print(f"✗ Failed to create subdirectory: {e}")
                                        failed_count += 1
                                        continue
                                
                                sub_uploaded, sub_failed, sub_converted = upload_recursive(local_item, remote_item)
                                uploaded_count += sub_uploaded
                                failed_count += sub_failed
                                converted_count += sub_converted
                                
                        except Exception as e:
                            print(f"✗ Failed to process {item.name}: {e}")
                            failed_count += 1
                            
                except Exception as e:
                    print(f"✗ Error during directory traversal: {e}")
                    failed_count += 1
                
                return uploaded_count, failed_count, converted_count
            
            print(f"Starting upload with automatic dos2unix conversion...")
            uploaded, failed, converted = upload_recursive(local_path, target_dir)
            
            print(f"\n=== UPLOAD SUMMARY ===")
            print(f"✓ Successfully uploaded: {uploaded} files")
            if converted > 0:
                print(f"Files converted (dos2unix): {converted}")
            else:
                print(f"No text files required conversion")
            if failed > 0:
                print(f"✗ Failed uploads: {failed} files")
                print(f"Upload completed with errors.")
                return False
            else:
                print(f"All files uploaded successfully!")
            
            print(f"Target location: {target_dir}")

            try:
                target_ssh = connection_cache['target_ssh']
                stdin, stdout, stderr = target_ssh.exec_command(f'ls -la "{target_dir}" | head -10')
                verification = stdout.read().decode()
                print(f"\nFirst few files in target directory:")
                print(verification)

                if converted > 0:
                    print(f"\nVerifying line endings conversion...")
                    text_exts = "*.txt -o -name *.sh -o -name *.py -o -name *.conf -o -name *.log"
                    stdin, stdout, stderr = target_ssh.exec_command(f'find "{target_dir}" -type f \\( -name {text_exts} \\) | head -3 | xargs file 2>/dev/null || echo "No text files found for verification"')
                    file_check = stdout.read().decode()
                    if file_check.strip():
                        print("File type verification (should show 'text' without 'CRLF'):")
                        print(file_check)
                        
            except Exception as e:
                print(f"Note: Could not verify upload: {e}")
            
            return failed == 0
            
        except Exception as e:
            print(f"✗ Upload failed completely: {e}")
            return False
    

    while True:
        try:
            print(f"\n{'='*50}")
            print(f"UPLOAD ATTEMPT - {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*50}")
            
            if not connection_cache.get('jump_host'):
                jump_host, jump_user, target_host, target_user = get_connection_details()
                connection_cache.update({
                    'jump_host': jump_host,
                    'jump_user': jump_user, 
                    'target_host': target_host,
                    'target_user': target_user
                })
            else:
                jump_host = connection_cache['jump_host']
                jump_user = connection_cache['jump_user']
                target_host = connection_cache['target_host'] 
                target_user = connection_cache['target_user']

            while not connection_cache['connected']:
                print("Establishing connection...")
                
                if safe_connect(jump_host, jump_user, target_host, target_user):
                    break
                else:
                    print(f"Connection failed. Waiting 5 seconds before retry...")
                    time.sleep(5)

            try:
                connection_cache['target_ssh'].exec_command('echo "Connection test"')
                print("✓ Connection is healthy")
            except:
                print("Connection lost. Reconnecting...")
                safe_disconnect()
                continue

            local_path = get_local_path()
            target_path = get_target_path()
            upload_success = perform_upload(local_path, target_path)
            
            if upload_success:
                print(f"\nUPLOAD COMPLETED SUCCESSFULLY!")
                try:
                    exit_choice = input("\nUpload successful! Exit program? (y/n): ").strip().lower()
                    if exit_choice in ['y', 'yes']:
                        break
                    else:
                        print("Continuing for another upload...")
                        continue
                except KeyboardInterrupt:
                    print("\nContinuing for another upload...")
                    continue
            else:
                print(f"\nUpload failed or incomplete. Trying again...")
                try:
                    retry_choice = input("Retry upload? (y/n): ").strip().lower()
                    if retry_choice not in ['y', 'yes']:
                        break
                except KeyboardInterrupt:
                    print("\nRetrying upload...")
                continue
            
        except KeyboardInterrupt:
            print(f"\n\nProgram interrupted by user (Ctrl+C)")
            try:
                exit_choice = input("Do you want to exit? (y/n): ").strip().lower()
                if exit_choice in ['y', 'yes']:
                    break
                else:
                    print("Continuing...")
                    continue
            except KeyboardInterrupt:
                print("\nForcing continue...")
                continue
                
        except Exception as e:
            print(f"\n✗ Unexpected error: {e}")
            print("The program will continue running...")
            time.sleep(2)
            continue
    
    safe_disconnect()
    print("Program ended. All connections closed.")

if __name__ == "__main__":
    upload_via_jump_server_continuous()