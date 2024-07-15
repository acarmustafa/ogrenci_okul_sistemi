import psycopg2 as psy
from psycopg2 import sql,extensions
from datetime import datetime
database='öğrenci'
user='postgres'
password='mustafa'
host='localhost'
port='5432'
connection=psy.connect(database=database,user=user,password=password,host=host,port=port)
cursor=connection.cursor()
autocommit=extensions.ISOLATION_LEVEL_AUTOCOMMIT
connection.set_isolation_level(autocommit)
def createDB(dbname):
    try:
        connection=psy.connect(
          dbname='postgres',
          user=user,
          password=password,
          host=host,
          port=port
        )
        
        connection.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT) 
        cursor=connection.cursor()
        cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{dbname}'")
        exists = cursor.fetchone()
        
        if not exists:
            create_db = sql.SQL('CREATE DATABASE {}').format(sql.Identifier(dbname))
            cursor.execute(create_db)
            print(f'{dbname} başarıyla oluşturuldu.')
        else:
            print(f'{dbname} adlı veritabanı zaten var.')
        
        
        print(f'{dbname} başarıyla oluşturuldu')  
    
        
    except(Exception,psy.Error) as error:
        print(f'veritabanı oluşturulurken hata oluştu: {error}')
    finally:   
        if connection:
            cursor.close()
            connection.close()
            print('PostgreSQL veri tabanı kapatılmıştır.') 
def createTable():
    try:
        connection=psy.connect(
          dbname='öğrenci',
          user=user,
          password=password,
          host=host,
          port=port
        )
        
        connection.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT) 
        cursor=connection.cursor() 
        create_öğrenci_table='''
        CREATE TABLE IF NOT EXISTS öğrenci(
           id SERIAL PRIMARY KEY,
           isim VARCHAR(100) NOT NULL,
           soysisim VARCHAR(100) NOT NULL,
           doğum_tarihi DATE NOT NULL,
           cinsiyet VARCHAR(100) NOT NULL,
           telefon INTEGER  NOT NULL,
           sınıf_id INTEGER 
           );
        '''  
        create_branş_table='''
        CREATE TABLE IF NOT EXISTS branş(
            id SERIAL PRIMARY KEY,
            isim VARCHAR(100) NOT NULL
        );
        
        ''' 
        create_öğretmen_table='''
        CREATE TABLE IF NOT EXISTS öğretmen(
            id SERIAL PRIMARY KEY,
            isim VARCHAR(100) NOT NULL,
            soyisim VARCHAR(100) NOT NULL,
            branş_id INTEGER,
            doğum_tarihi DATE,
            cinsiyet VARCHAR(100) NOT NULL,
            telefon INTEGER
            
        );
        
        ''' 
        create_sınıf_table='''
        CREATE TABLE IF NOT EXISTS sınıf(
            id SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            kat INTEGER
            
        );
       
        
        
        ''' 
        cursor.execute(create_öğrenci_table) 
        cursor.execute(create_branş_table,)
        cursor.execute(create_öğretmen_table)
        cursor.execute(create_sınıf_table)
        print('Tablolar başarıyla oluşturuldu.')
        
        
        print('Tablolar başarıyla oluşturuldu ve sütun eklendi.')
    except(Exception,psy.Error) as error:
        print(f'veritabanı oluşturulurken hata oluştu: {error}')
    finally:   
        if connection:
            cursor.close()
            connection.close()
            print('PostgreSQL veri tabanı kapatılmıştır.') 
def insertTable(table,values):
      try:
        connection=psy.connect(
          dbname='öğrenci',
          user=user,
          password=password,
          host=host,
          port=port
        )
        
        connection.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT) 
        cursor=connection.cursor()
        if table =='öğrenci':
            insert_query='''INSERT INTO öğrenci(isim,soysisim,doğum_tarihi,cinsiyet,telefon,sınıf_id)
            VALUES(%s,%s,%s,%s,%s,%s);
            
            '''
        elif table=='branş':
            insert_query='''INSERT INTO branş(isim)
            VALUES(%s); 
            '''  
        elif table=='öğretmen':
            insert_query='''INSERT INTO öğretmen(isim,soyisim,branş_id,doğum_tarihi,cinsiyet,telefon)
            VALUES(%s,%s,%s,%s,%s,%s);
            '''  
        elif table=='sınıf':
            insert_query='''INSERT INTO sınıf(name,kat)
            VALUES(%s,%s);
            
            '''  
        cursor.execute(insert_query,values)
        print('Kayıt tabloya başarıyla eklenmiştir.')          
                  
      except(Exception,psy.Error) as error:
        print(f'veritabanı oluşturulurken hata oluştu: {error}')
      finally:   
        if connection:
            cursor.close()
            connection.close()
            print('PostgreSQL veri tabanı kapatılmıştır.')           
def selectTable(table):
    try:
        connection=psy.connect(
          dbname='öğrenci',
          user=user,
          password=password,
          host=host,
          port=port
        )
        connection.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT) 
        cursor=connection.cursor()
        if table=='öğrenci':
           select_query='SELECT * FROM öğrenci' 
        elif table=='branş':
           select_query='SELECT * FROM branş'
        elif table=='öğretmen':
           select_query='SELECT * FROM öğretmen'
        elif table=='sınıf':
           select_query='SELECT * FROM sınıf' 
        cursor.execute(select_query)
        rows=cursor.fetchall()
        print('Liste oluşturuldu.')
        for row in rows:
           print(row)
    except(Exception,psy.Error) as error:
        print(f'veritabanı oluşturulurken hata oluştu: {error}')
    finally:   
        if connection:
            cursor.close()
            connection.close()
            print('PostgreSQL veri tabanı kapatılmıştır.') 
def updateTable(table,set_values,condition) :
    try:
        connection=psy.connect(
          dbname='öğrenci',
          user=user,
          password=password,
          host=host,
          port=port
        )
        connection.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT) 
        cursor=connection.cursor()
        if table=='öğrenci':
            update_query='''UPDATE öğrenci SET isim=%s,soysisim=%s,doğum_tarihi=%s,cinsiyet=%s,telefon=%s,sınıf_id=%s WHERE id=%s;'''
        elif table=='branş':
            update_query='''UPDATE branş SET isim=%s WHERE id=%s;''' 
        elif table=='öğretmen':
            update_query='''UPDATE öğretmen SET isim=%s,soyisim=%s,branş_id=%s,doğum_tarihi=%s,cinsiyet=%s,telefon=%s WHERE id=%s;'''   
        elif table=='sınıf':
            update_query='''UPDATE sınıf SET name=%s,kat=%s WHERE id=%s;'''  
        cursor.execute(update_query,set_values+(condition,))
        print('Kayıt başarıyla güncellendi.')          
        
    except(Exception,psy.Error) as error:
        print(f'veritabanı oluşturulurken hata oluştu: {error}')
    finally:   
        if connection:
            cursor.close()
            connection.close()
            print('PostgreSQL veri tabanı kapatılmıştır.')
def deleteTable(table,id):
    try:
        connection=psy.connect(
          dbname='öğrenci',
          user=user,
          password=password,
          host=host,
          port=port
        )
        connection.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT) 
        cursor=connection.cursor()
        if table=='öğrenci':
            deletequery='''DELETE FROM öğrenci WHERE id=%s'''
        elif table=='branş':
            deletequery='''DELETE FROM branş WHERE id=%s '''  
        elif table=='öğretmen':
            deletequery='''DELETE FROM öğretmen WHERE id=%s'''
        elif table=='sınıf':
            deletequery='''DELETE FROM sınıf WHERE id=%s'''
        cursor.execute(deletequery,(id,))
        print('Tablo başarıyla silindi. ')              
    except(Exception,psy.Error) as error:
        print(f'veritabanı oluşturulurken hata oluştu: {error}')
    finally:   
        if connection:
            cursor.close()
            connection.close()
            print('PostgreSQL veri tabanı kapatılmıştır.')

                
def menu():
    print('Hoşgeldiniz, seçiminizi yapınız:')
    print('1 - Yeni kayıt ekle')
    print('2 - Kayıt güncelleme')
    print('3 - Kayıt silme')
    print('4 - Kayıtları listeleme')
    print('5 - sınıf bilgisi ve doğum tarihi alınarak 2000 yılından önce ve sonra doğanlar')  
    print('6 - İsmi girilen öğrencinin hangi katta olduğu ')  
def insert_case():
    
    print('Öğrenci için: ')
    print('Eklemek istemiyorsanız q tuşuna basın.')
    isim=input('ISIM: ') 
    if isim=='q':
        pass
    else:
        soyisim=input('SOYISIM: ') 
        doğum_tarihi=input('DOĞUM TARIHI: ')
        cinsiyet= input('CINSIYET: ')
        telefon=input('TELEFON: ')
        sınıf_id=input('SINIF_ID: ') 
        insertTable('öğrenci',(isim,soyisim,doğum_tarihi,cinsiyet,telefon,sınıf_id))
    print('Branş için: ')
    print('Eklemek istemiyorsanız q tuşuna basın.')
    isim=input('ISIM: ') 
    if isim=='q':
        pass
    else:
        insertTable('branş',(isim,))
    print('Öğretmen için: ') 
    print('Eklemek istemiyorsanız q tuşuna basın.') 
    isim=input('ISIM: ') 
    if isim=='q':
        pass 
    else:
        soyisim=input('SOYISIM: ')
        branş_id=input('BRANS_ID: ') 
        doğum_tarihi=input('DOGUM_TARIHI: ')
        cinsiyet=input('CINSIYET: ')
        telefon=input('TELEFON: ')
        insertTable('öğretmen',(isim,soyisim,branş_id,doğum_tarihi,cinsiyet,telefon))
    print('Sınıf için: ')
    print('Eklemek istemiyorsanız q tuşuna basın.')
    isim=input('ISIM: ')
    if isim=='q':
        pass
    else:
        kat=input('KAT: ')
        insertTable('sınıf',(isim,kat))
def update_case():
    
    print('Öğrenci için: ')
    print('Güncelleme istemiyorsanız q tuşuna basın.')
    record_id = input('RECORD_ID: ')
    if record_id=='q':
        pass
    else:
        id=input('ID: ')
        isim=input('ISIM: ') 
        soyisim=input('SOYISIM: ') 
        doğum_tarihi=input('DOĞUM TARIHI: ')
        cinsiyet= input('CINSIYET: ')
        telefon=input('TELEFON: ')
        sınıf_id=input('SINIF_ID: ') 
        updateTable('öğrenci',(isim,soyisim,doğum_tarihi,cinsiyet,telefon,sınıf_id),id)
    print('Branş için: ')
    print('Güncelleme istemiyorsanız q tuşuna basın.')
    id=input('ID: ')
    if id=='q':
        pass
    else:
        isim=input('ISIM: ') 
        updateTable('branş',(isim,),id)
    print('Öğretmen için: ') 
    print('Güncelleme istemiyorsanız q tuşuna basın.')
    id=input('ID: ') 
    if id=='q':
        pass
    else:
        isim=input('ISIM: ')  
        soyisim=input('SOYISIM: ')
        branş_id=input('BRANS_ID: ') 
        doğum_tarihi=input('DOGUM_TARIHI: ')
        cinsiyet=input('CINSIYET: ')
        telefon=input('TELEFON: ')
        updateTable('öğretmen',(isim,soyisim,branş_id,doğum_tarihi,cinsiyet,telefon),id)
    print('Sınıf için: ')
    print('Güncelleme istemiyorsanız q tuşuna basın.')
    id=input('ID: ')
    if id=='q':
        pass
    else:
        isim=input('ISIM: ')
        kat=input('KAT: ')
        updateTable('sınıf',(isim,kat),id)
def delete_case():
    print('Öğrenci için: ')
    print('Silmek istemiyorsanız q tuşuna basın')
    id=input('ID:'  )
    if id=='q':
        pass
    else:
        deleteTable('öğrenci',(id,))
    print('Branş için: ')
    print('Silmek istemiyorsanız q tuşuna basın')
    id=input('ID:'  )
    
    if id=='q':
        pass
    else:
        deleteTable('Branş',(id,))
    print('Öğretmen için: ')
    print('Silmek istemiyorsanız q tuşuna basın')
    id=input('ID:'  )
    if id=='q':
        pass
    else:
        deleteTable('öğretmen',(id,))
    print('Sınıf için: ')
    print('Silmek istemiyorsanız q tuşuna basın')
    id=input('ID:'  )
    if id=='d':
        pass
    else:
        deleteTable('sınıf',(id,))
def list_case() :
    selectTable('öğrenci')
    selectTable('branş')
    selectTable('öğretmen')
    selectTable('sınıf') 
        
def filter_by_class_and_birth():
    try:
        connection=psy.connect(
          dbname='öğrenci',
          user=user,
          password=password,
          host=host,
          port=port
        )
        connection.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT) 
        cursor=connection.cursor()
        sınıf=input('Sınıf bilgisini giriniz: ')
        query_sınıf_id='''SELECT id FROM sınıf WHERE name=%s;'''
        cursor.execute(query_sınıf_id,(sınıf,))
        sınıf_id=cursor.fetchone()
        if sınıf_id:
            sınıf_id = sınıf_id[0]
        else:
            print(f"{sınıf} isimli sınıf veritabanında bulunamadı.")
            return
        
        query_student='''SELECT isim,soysisim,doğum_tarihi FROM öğrenci
        WHERE sınıf_id=%s
        '''
        cursor.execute(query_student,(sınıf_id,))
        
        records=cursor.fetchall()
        before_2000=[]
        after_2000=[]
        for record in records:
            doğum_tarihi=record[2]
            
            if doğum_tarihi<datetime(2000,1,1).date():
                before_2000.append(record)
            else:
                after_2000.append(record)
        print(f'{sınıf} sınıfındaki öğrenciler ve doğum tarihleri:')
        for record in records:
            print(record)        
        print('2000 yılıdan önce doğanlar')
        for record in before_2000:
            print(record)   
        print('2000 yılıdan sonra doğanlar') 
        for record in after_2000:
            print(record)            
    except(Exception,psy.Error) as error:
        print(f'veritabanı oluşturulurken hata oluştu: {error}')
    finally:   
        if connection:
            cursor.close()
            connection.close()
            print('PostgreSQL veri tabanı kapatılmıştır.')  
def find_student_floor():
    try:
        connection=psy.connect(
          dbname='öğrenci',
          user=user,
          password=password,
          host=host,
          port=port
        )
        connection.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT) 
        cursor=connection.cursor()
        isim=input('Öğrencinin ismini giriniz: ').strip().lower()
        query_sınıf_id='''SELECT sınıf_id FROM öğrenci WHERE isim=%s
        '''
        cursor.execute(query_sınıf_id,(isim,))
        
        sınıf_id=cursor.fetchone()
        if sınıf_id:
            sınıf_id=sınıf_id[0]
        else:
            print(f"{isim} isimli öğrenci veritabanında bulunamadı.")
            return    
        query_kat='''SELECT kat FROM sınıf WHERE id=%s
        '''
        cursor.execute(query_kat,(sınıf_id,))
        kat=cursor.fetchone()
        if kat:
            kat=kat[0]
            print(f'{isim} isimli öğrenci {kat}. katta.')
        else:
            print(f'{isim} isimli öğrencinin sınıfı veri tabanında bulunamadı. ')
    except(Exception,psy.Error) as error:
        print(f'veritabanı oluşturulurken hata oluştu: {error}')
    finally:   
        if connection:
            cursor.close()
            connection.close()
            print('PostgreSQL veri tabanı kapatılmıştır.')    
                  
           
    
  
    


def switch_case(case) :
    switcher={
        '1':insert_case,
        '2':update_case,
        '3':delete_case,
        '4':list_case,
        '5':filter_by_class_and_birth,
        '6':find_student_floor
        
        
    }
    return switcher.get(case, lambda: print("Geçersiz seçenek"))

    
def main():
    createDB('öğrenci')
    createTable()
    while True:
        menu()
        case=input('Lütfen seçiminiz yapınız: ') 
        func=switch_case(case) 
        func() 
    
                                               
    
if __name__=='__main__':
   main()
    
    
    
     
    
             
        