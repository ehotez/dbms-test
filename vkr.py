import tkinter as tk
from tkinter import ttk
from clickhouse_driver import Client
import psycopg2
import threading
import configparser
import datetime
import csv
import psutil
import numpy as np

# Инициализируем окно приложения
root = tk.Tk()
root.geometry("850x700")
root.title("DBMS test")

# Глобальная переменная для остановки тестирования
global stop
stop = False

# Создание сущности для вкладок
tab_control = ttk.Notebook(root)

ch_tab = ttk.Frame(tab_control)
tab_control.add(ch_tab, text="ClickHouse")

pg_tab = ttk.Frame(tab_control)
tab_control.add(pg_tab, text="PostgreSQL")

tab_control.pack(expand=1, fill="both")

# Функция инициализации каждой вкладки
def create_forms(tab, dbms):
    log_label = tk.Label(tab, text="Логин:")
    log_label.grid(column=0, row=0, padx=5, pady=5)
    login = tk.Entry(tab)
    login.grid(column=1, row=0, padx=5, pady=5)

    pass_label = tk.Label(tab, text="Пароль:")
    pass_label.grid(column=0, row=1, padx=5, pady=5)
    password = tk.Entry(tab, show="*")
    password.grid(column=1, row=1, padx=5, pady=5)

    host_label = tk.Label(tab, text="Хост:")
    host_label.grid(column=0, row=2, padx=5, pady=5)
    host = tk.Entry(tab)
    host.grid(column=1, row=2, padx=5, pady=5)

    port_label = tk.Label(tab, text="Порт:")
    port_label.grid(column=0, row=3, padx=5, pady=5)
    port = tk.Entry(tab)
    port.grid(column=1, row=3, padx=5, pady=5)

    db_label = tk.Label(tab, text="База данных:")
    db_label.grid(column=0, row=4, padx=5, pady=5)
    db = tk.Entry(tab)
    db.grid(column=1, row=4, padx=5, pady=5)

    # Открываем файл конфига
    config = configparser.ConfigParser()
    config.read(dbms+'.ini')

    connect_button = tk.Button(tab, text="Подключиться", command=lambda: connection(
                        login, password, host, port, db, tab, dbms))
    connect_button.grid(column=1, row=5, padx=5, pady=5)

    # Получаем данные из конфига если они есть
    try:
        login.insert(0, config[dbms]['username'])
        db.insert(0, config[dbms]['database'])
        password.insert(0, config[dbms]['password'])
        host.insert(0, config[dbms]['host'])
        port.insert(0, config[dbms]['port'])
    except:
        pass

def connection(*params): # Подключаемся к СУБД

    user = params[0]
    password = params[1]
    host = params[2]
    port = params[3]
    db = params[4]
    tab = params[5]
    dbms = params[6]

    conf_label = tk.Label(tab)
    conf_label.grid(column=1, row=6)

    if dbms == 'ClickHouse': # К ClickHouse
        try:
            client = Client(host = host.get(),
                            user = user.get(), 
                            password = password.get(),
                            database = db.get())
            client.execute('Select version()')
            print(client.last_query.elapsed)
        except Exception:
            print('fail - ClickHouse')
            conf_label.config(text='Unable to connect')
            return
        
    elif dbms == 'PostgreSQL': # К PostgreSQL
        try:
            conn = psycopg2.connect(user = user.get(),
                                password = password.get(),
                                database = db.get(),
                                host = host.get(),
                                port = port.get())
            cursor = conn.cursor()
            cursor.execute("explain analyze SELECT version();") 
            record = cursor.fetchall()
            print(record[2])
        except Exception:
            print('fail - PostgreSQL')
            conf_label.config(text='Unable to connect')
            return   
    
    #Сохраняем данные в файле конфига
    config = configparser.ConfigParser()
    config[dbms] = {'username': user.get(),
            'password': password.get(),
            'host': host.get(),
            'port': port.get(),
            'database': db.get()}
    
    with open(dbms+'.ini', 'w') as configfile:
        config.write(configfile)

    # Убираем поля для настройки подключения к СУБД
    for widget in tab.grid_slaves():
        col = int(widget.grid_info()["column"])
        if col == 0 or col == 1:
            widget.grid_forget()

    # Описания каждого сценарного запроса
    label_names ={
        'select': 'Выбирает заданное количество строк',
        'group': 'Группировка \n (по выбранной колонке)',
        'sort': 'Выберите тип сортировки \n (по выбранной колонке)',
        'between': 'Введите диапазон значений \n (для выбранной колонки)',
        'like': 'Введите шаблон \n (для выбранной колонки)',
        'in': 'Введите значения через запятую \n (для выбранной колонки)',
        'not_in': 'Введите значения через запятую \n (для выбранной колонки)',
        'my': 'Введите собственный запрос \n (учитывается только параметр "Кол-во испытаний")'
    }

    global col_names
    col_names = []
    
    # Функция загрузки всех доступных колонок в таблице для последующего выбора
    def load_columns():
        global col_names
        if dbms == 'ClickHouse':
            result = client.execute(f"DESCRIBE TABLE {table.get()};")
            col_names = [row[0] for row in result if len(row) > 0]
            column['values'] = col_names
        elif dbms == 'PostgreSQL':
            cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table.get()}';") 
            col_names = cursor.fetchall()
            column['values'] = col_names
        column.current(0)

    tk.Label(tab, text="Таблица:").grid(column=2, row=0, padx=5, pady=5)
    table = ttk.Combobox(tab, state='readonly')
    table.grid(column=3, row=0, padx=5, pady=5)
    table.bind("<<ComboboxSelected>>", load_columns)

    tk.Label(tab, text="Колонка:").grid(column=2, row=1, padx=5, pady=5)
    column = ttk.Combobox(tab, state='readonly')
    column.grid(column=3, row=1, padx=5, pady=5)

    # Получаем список таблиц в базе данных и используем их для выбора
    if dbms == 'ClickHouse':
        result = client.execute(f"SELECT name FROM system.tables WHERE database = '{db.get()}'")
        table['values'] = result
    elif dbms == 'PostgreSQL':
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'") 
        result = cursor.fetchall()
        table['values'] = result
    
    table.current(0)
    load_columns()

    tk.Label(tab, text="Размерность запроса \n (кол-во строк):").grid(column=2, row=2, padx=5, pady=5)
    rows = tk.Spinbox(tab, from_=1, to=1000000)
    rows.insert(1,0)
    rows.grid(column=3, row=2, padx=5, pady=5)

    tk.Label(tab, text="Кол-во испытаний:").grid(column=2, row=3, padx=5, pady=5)
    repeats = tk.Spinbox(tab, from_=1, to=1000000)
    repeats.insert(1,0)
    repeats.grid(column=3, row=3, padx=5, pady=5)

    radio_var = tk.StringVar()
    radio_var.set('group')
    r_sort = tk.StringVar()
    r_sort.set('desc')
    range_var = tk.BooleanVar()
    range_var.set(False)

    # При использовании временного интервала показываем необходимые поля
    def time_trig():
        if range_var.get():
            time_label.grid(column=2,row=11)
            time_col.grid(column=3, row=11)
            start_label.grid(column=2, row=12)
            start_time.grid(column=3,row=12)
            end_label.grid(column=2,row=13)
            end_time.grid(column=3, row=13)
        else:
            time_label.grid_forget()
            end_label.grid_forget()
            start_label.grid_forget()
            time_col.grid_forget()
            start_time.grid_forget()
            end_time.grid_forget()

    timebox = tk.Checkbutton(tab, text='Временной \n промежуток', variable=range_var, onvalue=True, offvalue=False, command=time_trig)
    timebox.grid(column=2, row=10)

    time_col = ttk.Combobox(tab, values=col_names, state='readonly')
    time_label = tk.Label(tab, text='Столбец:')
    time_col.current(0)
    start_label = tk.Label(tab, text='Начало периода:')
    end_label = tk.Label(tab, text='Конец периода:')
    start_time = tk.Entry(tab)
    end_time = tk.Entry(tab)


    tk.Label(tab, text="Доп. параметры:").grid(column=4, row=4, padx=5, pady=5, columnspan=2)
    add_l = tk.Label(tab)
    add_l.grid(column=4, row=5,columnspan=2)
    default_entry = tk.Entry(tab)
    start_entry = tk.Entry(tab)
    end_entry = tk.Entry(tab)
    my_query_entry = tk.Text(tab, height=15, width=40)
    rdesc = tk.Radiobutton(tab, text='DESC', variable=r_sort, value='desc')
    rasc = tk.Radiobutton(tab, text='ASC', variable=r_sort, value='asc')
    status = tk.Label(tab, text='')
    status.grid(column=2, row=17)

    # Функция отображения доп. настроек для каждого сценарного запроса
    def show_fields():
        for widget in tab.grid_slaves():
            col = int(widget.grid_info()["column"])
            row = int(widget.grid_info()["row"])
            if (col == 4 or col == 5) and (row > 5):
                widget.grid_forget()
        
        key = radio_var.get()
        add_l.config(text=label_names[key])
        if(key == 'between'):
            tk.Label(tab, text='Start').grid(column=4, row=6)
            start_entry.grid(column=5, row=6)
            tk.Label(tab, text='Finish').grid(column=4, row=7)
            end_entry.grid(column=5, row=7)
        elif(key == 'group'):
            pass
        elif(key == 'sort'):
            rdesc.grid(column=4, row=6)
            rasc.grid(column=5, row=6)
        elif(key == 'select'):
            pass
        elif(key == 'my'):
            my_query_entry.grid(column=4, row=6, columnspan=5, rowspan=1000)
        else:
            default_entry.delete(0, tk.END)
            default_entry.grid(column=4, row=6, columnspan=2)

    show_fields() 

    rbutton3 = tk.Radiobutton(tab, text="GROUP BY", variable=radio_var, value="group", command=show_fields)
    rbutton3.grid(column=2, row=4, padx=5, pady=5)

    rbutton4 = tk.Radiobutton(tab, text="ORDER BY", variable=radio_var, value="sort", command=show_fields)
    rbutton4.grid(column=2, row=5, padx=5, pady=5)

    rbutton5 = tk.Radiobutton(tab, text="BETWEEN", variable=radio_var, value="between", command=show_fields)
    rbutton5.grid(column=2, row=6, padx=5, pady=5)

    rbutton5 = tk.Radiobutton(tab, text="LIKE", variable=radio_var, value="like", command=show_fields)
    rbutton5.grid(column=2, row=7, padx=5, pady=5)

    rbutton6 = tk.Radiobutton(tab, text="IN", variable=radio_var, value="in", command=show_fields)
    rbutton6.grid(column=3, row=4, padx=5, pady=5)

    rbutton7 = tk.Radiobutton(tab, text="NOT IN", variable=radio_var, value="not_in", command=show_fields)
    rbutton7.grid(column=3, row=5, padx=5, pady=5)

    rbutton8 = tk.Radiobutton(tab, text="SELECT", variable=radio_var, value="select", command=show_fields)
    rbutton8.grid(column=3, row=6, padx=5, pady=5)

    rbutton = tk.Radiobutton(tab, text="Собственный запрос", variable=radio_var, value="my", command=show_fields)
    rbutton.grid(column=3, row=7, padx=5, pady=5)
    
    # Инициализация тестирования
    def testing():
        global stop
        stop = False
        query = ''
        key = radio_var.get()
        table_name = table.get()
        col_name = column.get()
        range_col = time_col.get()
        N = rows.get()
        repeat = int(repeats.get())
        query_type = ''
        query_time = np.array([])
        query_cpu = np.array([])
        query_mem_proc = np.array([])
        query_mem_bytes = np.array([])
        results = []
        description = ''
        use_range = range_var.get()

        # Формируем запрос в зависимости от заданных параметров
        if use_range:
            s_range = start_time.get()
            e_range = end_time.get()
            if key == 'group':
                description = 'GROUP BY'
                query = f'''SELECT {col_name}, COUNT(*)
                            FROM {table_name}
                            WHERE {range_col} BETWEEN {s_range} AND {e_range} 
                            GROUP BY {col_name}
                            LIMIT {N}; '''
            elif key == 'sort':
                description = 'ORDER BY'
                query = f'''SELECT * 
                            FROM {table_name}
                            WHERE {range_col} BETWEEN {s_range} AND {e_range}
                            ORDER BY {col_name} {r_sort.get()}
                            LIMIT {N}; '''
            elif key == 'between':
                description = 'BETWEEN'
                query = f'''SELECT *
                            FROM {table_name}
                            WHERE {col_name} BETWEEN {start_entry.get()} AND {end_entry.get()} AND
                            {range_col} BETWEEN {s_range} AND {e_range}
                            LIMIT {N}; '''
            elif key == 'like':
                description = 'LIKE'
                query = f'''SELECT *
                            FROM {table_name}
                            WHERE {col_name} LIKE '{default_entry.get()}' AND {range_col} BETWEEN {s_range} AND {e_range}
                            LIMIT {N}; '''
            elif key == 'in':
                description = 'IN'
                query = f'''SELECT *
                            FROM {table_name}
                            WHERE {col_name} IN ({default_entry.get()}) AND {range_col} BETWEEN {s_range} AND {e_range}
                            LIMIT {N}; '''
            elif key == 'not_in':
                description = 'NOT IN'
                query = f'''SELECT *
                            FROM {table_name}
                            WHERE {col_name} NOT IN ({default_entry.get()}) AND {range_col} BETWEEN {s_range} AND {e_range}
                            LIMIT {N}; '''
            elif key == 'select':
                description = 'SELECT'
                query = f'''SELECT {col_name}
                            FROM {table_name}
                            WHERE {range_col} BETWEEN {s_range} AND {e_range}
                            LIMIT {N}; '''
            elif key == 'my':
                description = 'Собственный запрос'
                query = my_query_entry.get("1.0", "end-1c")
            description = description + f' с диапазоном значений {range_col} от {s_range} до {e_range}'

        else:
            if key == 'group':
                description = 'GROUP BY'
                query = f'''SELECT {col_name}, COUNT(*)
                            FROM {table_name}
                            GROUP BY {col_name}
                            LIMIT {N}; '''
            elif key == 'sort':
                description = 'ORDER BY'
                query = f'''SELECT * 
                            FROM {table_name}
                            ORDER BY {col_name} {r_sort.get()}
                            LIMIT {N}; '''
            elif key == 'between':
                description = 'BETWEEN'
                query = f'''SELECT *
                            FROM {table_name}
                            WHERE {col_name} BETWEEN {start_entry.get()} AND {end_entry.get()}
                            LIMIT {N}; '''
            elif key == 'like':
                description = 'LIKE'
                query = f'''SELECT *
                            FROM {table_name}
                            WHERE {col_name} LIKE '{default_entry.get()}'
                            LIMIT {N}; '''
            elif key == 'in':
                description = 'IN'
                query = f'''SELECT *
                            FROM {table_name}
                            WHERE {col_name} IN ({default_entry.get()})
                            LIMIT {N}; '''
            elif key == 'not_in':
                description = 'NOT IN'
                query = f'''SELECT *
                            FROM {table_name}
                            WHERE {col_name} NOT IN ({default_entry.get()})
                            LIMIT {N}; '''
            elif key == 'select':
                description = 'SELECT'
                query = f'''SELECT {col_name}
                            FROM {table_name}
                            LIMIT {N}; '''
            elif key == 'my':
                description = 'Собственный запрос'
                query = my_query_entry.get("1.0", "end-1c")

        # Формируем описание будущего файла с результатами тестирования
        print(query)
        headers = [
            [],
            [f'Формат запроса: {description}'],
            [],
            [f'Таблица: {table_name}'],
            [f'Столбец: {col_name}'],
            [f'Количество испытаний: {repeat}'],
            [f'Размерность запроса (кол-во строк): {N}'],
            [],
            [f'Запрос: {str(query)}'],
            [],
            ['№ испытания', 'Время выполнения (сек)', 'Cpu usage (%)', 'Memory usage (%)', 'Memory_usage (bytes)']
        ]

        current_time = datetime.datetime.now()
        range_col = current_time.strftime("%m-%d_%H-%M-%S")

        if dbms == 'ClickHouse':
            filename = range_col + '_(CH)'
        elif dbms == 'PostgreSQL':
            filename = range_col + '_(PG)'
        
        # Функция тестирования СУБД ClickHouse
        def clickhouse_test(query):
            process = psutil.Process()
            start_memory_percent = process.memory_percent()
            start_memory_usage = process.memory_info().rss
            start_cpu_percent = psutil.cpu_percent(interval=None)

            for i in range(repeat):
                status.config(text=f'Progress {i+1} in {repeat}', foreground='black')

                client.execute(query)

                q_time = client.last_query.elapsed
                print(client.last_query.profile_info.bytes)

                end_cpu_percent = psutil.cpu_percent(interval=None)
                end_memory_percent = process.memory_percent()
                end_memory_usage = process.memory_info().rss
                cpu_usage = abs(end_cpu_percent - start_cpu_percent)
                memory_usage_percent = abs(end_memory_percent - start_memory_percent)
                memory_usage_bytes = abs(end_memory_usage - start_memory_usage)

                results.append([i+1, q_time, cpu_usage, memory_usage_percent, memory_usage_bytes])
                if stop:
                    status.config(text='Stopped')
                    break
                #sleep(2)

        # Функция тестирования СУБД PostgreSQL
        def postgres_test(query):
            process = psutil.Process()
            start_memory_percent = process.memory_percent()
            start_memory_usage = process.memory_info().rss
            start_cpu_percent = psutil.cpu_percent(interval=None)

            for i in range(repeat):
                status.config(text=f'Progress {i+1} in {repeat}', foreground='black')

                cursor.execute(query) 
                record = cursor.fetchall()

                end_cpu_percent = psutil.cpu_percent(interval=None)
                end_memory_percent = process.memory_percent()
                end_memory_usage = process.memory_info().rss
                cpu_usage = abs(end_cpu_percent - start_cpu_percent)
                memory_usage_percent = abs(end_memory_percent - start_memory_percent)
                memory_usage_bytes = abs(end_memory_usage - start_memory_usage)
                
                string = str(record[len(record)-1][0])
                string = string.split()
                q_time = float(string[2]) / 1000
                print(record)

                results.append([i+1, q_time, cpu_usage, memory_usage_percent, memory_usage_bytes])
                if stop:
                    status.config(text='Stopped')
                    break
                #sleep(2)

        start_button.grid_forget()
        stop_button.grid(column=2, row=16, padx=10, pady=10)
        # Начинаем тестирование в зависимости от СУБД
        if dbms == 'ClickHouse':
            try:
                clickhouse_test(query)
                passed = True
            except:
                status.config(text='Error')

                start_button.grid(column=2, row=16, padx=10, pady=10)
                stop_button.grid_forget()

                passed = False

        elif dbms == 'PostgreSQL':
            query ="explain analyze " + query
            try:
                postgres_test(query)
                passed = True
            except:
                status.config(text='Error')

                start_button.grid(column=2, row=16, padx=10, pady=10)
                stop_button.grid_forget()

                conn.rollback()
                passed = False

        #Если все тесты пройдены, расчитываем статистику и записываем результаты в csv файл
        if passed:

            file = open('results/' + filename + '.csv', mode='a', newline='')
            writer = csv.writer(file, delimiter=',')

            np_results = np.array(results)

            # Транспонирование массива
            transposed_array = np_results.T

            # Создание списка одномерных массивов для каждого столбца
            column_arrays = [np.array(column) for column in transposed_array]

            query_time = column_arrays[1]
            query_cpu = column_arrays[2]
            query_mem_proc = column_arrays[3]
            query_mem_bytes = column_arrays[4]

            statistic = [
                [],
                ['Среднее значение', np.mean(query_time), np.mean(query_cpu), np.mean(query_mem_proc), np.mean(query_mem_bytes)],
                ['Максимальное значение', np.max(query_time), np.max(query_cpu), np.max(query_mem_proc), np.max(query_mem_bytes)],
                ['Минимальное значение', np.min(query_time), np.min(query_cpu), np.min(query_mem_proc), np.min(query_mem_bytes)],
                ['Медиана', np.median(query_time), np.median(query_cpu), np.median(query_mem_proc), np.median(query_mem_bytes)],
                ['Стандартное отклонение', np.std(query_time), np.std(query_cpu), np.std(query_mem_proc), np.std(query_mem_bytes)],
                ['Дисперсия', np.var(query_time), np.var(query_cpu), np.var(query_mem_proc), np.var(query_mem_bytes)]
            ]

            writer.writerows(headers)
            writer.writerows(results)
            writer.writerows(statistic)

            file.close()
            status.config(text='Testing completed', foreground='green')
            start_button.grid(column=2, row=16, padx=10, pady=10)
            stop_button.grid_forget()

    # Функция начала тестирования в отдельном параллельном потоке
    def run_testing():
        t = threading.Thread(target=testing)
        t.start()

    # Функция остановки тестирования
    def stop_testing():
        global stop
        stop = True
        status.config(text='Stopping...')

    start_button = tk.Button(tab, text='Начать \n тестирование', command=run_testing)
    start_button.grid(column=2, row=16, padx=10, pady=10)
    stop_button = tk.Button(tab, text='Остановить \n тестирование', command=stop_testing)

# Создание 2-х отдельных вкладок для каждой СУБД
create_forms(ch_tab, "ClickHouse")
create_forms(pg_tab, "PostgreSQL")

# Функция при закрытии программы останавливает все текущие тесты
def on_closing():
    root.destroy()
    global stop
    stop = True   

root.protocol("WM_DELETE_WINDOW", on_closing)

root.mainloop()