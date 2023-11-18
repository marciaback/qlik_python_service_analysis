#!/usr/bin/env python
# coding: utf-8

# In[1]:


import gzip
import pandas as pd
from datetime import datetime


# In[2]:


##########################################################
# DESCOMPACTAÇÃO E CARGA DOS DADOS
##########################################################


# In[3]:


#descompactando o arquivo
gzip = gzip.open('apache.log.gz', 'rb')
log = open('apache.log', 'wb')
log.write( gzip.read() )
gzip.close()
log.close()


# In[4]:


#lendo os dados e colocando linha de cabeçalho (Combined Log Format)
dados = pd.read_csv('apache.log', sep = ' ', names = ['ip', 'hyphen', 'userid', 'id', 'time', 'request', 
                                                      'status', 'size', 'referer', 'user-agent'])
dados.head(2)


# In[5]:


##########################################################
# MANIPULAÇÃO DOS DADOS PARA PREPARÁ-LOS PARA AS ANÁLISES
##########################################################


# In[6]:


# transformando em dataframe
df = pd.DataFrame()
df = dados


# In[7]:


# criando a coluna endpoint splitando a coluna request
endpoint = df['request'].str.split("/", n = 1, expand = True)
endpoint = endpoint[1].str.split("/", n = 1, expand = True)
df['endpoint']= endpoint[0]
df.head(1)


# In[8]:


# criando a coluna browser splitando a coluna user-agent
browser = df['user-agent'].str.split(" ", n = 1, expand = True) #com versão
browser.head(5)
df['browser']= browser[0]
df.head(1)


# In[9]:


# criando a coluna do endereço ip classe C com base na coluna ip
def define_classe (string):
    if '.' in string[:3]:
        return False
    else:
        if string[:3] >= '192' and string[:3] <= '223':
            return True
        else:
            return False
    
ip = pd.DataFrame()
ip['ip'] = df['ip'].apply(str)

classe = pd.DataFrame()
classe['classec'] = ip['ip'].map(define_classe)

df['classec'] = classe['classec']
df.head(2)


# In[10]:


# criando a coluna hora com base na coluna time
hora = df['time'].str.split('T', n = 1, expand = True)
hora = hora[1].str.split(':', n = 1, expand = True)
df['hora']= hora[0]
df.head(1)


# In[11]:


# criando a coluna status_desc com base na coluna status
def define_status (string):
    if string[:1] == '1':
        return 'Continuar'
    elif string[:1] == '2':
        return 'Sucesso'
    elif string[:1] == '3':
        return 'Redirecionada'
    elif string[:1] == '4':
        return 'Erro de cliente'
    elif string[:1] == '5':
        return 'Erro no servidor'
    else:
        return '-'
    
status = pd.DataFrame()
status['status'] = df['status'].apply(str)

status_desc = pd.DataFrame()
status_desc['status_desc'] = status['status'].map(define_status)

df['status_desc'] = status_desc['status_desc']
df.head(2)


# In[12]:


#encontrando a diferença em horas e minutos do log analisado para calcular operações

max = df.time.max().replace('[','').replace(']','')[:-1]
min = df.time.min().replace('[','').replace(']','')[:-1]

dif = (datetime.fromisoformat(max) - datetime.fromisoformat(min))

#diferença em horas e minutos do inicio e fim do log
dif_horas = int(dif.total_seconds())/60/60 
dif_min = int(dif.total_seconds())/60

print('diferença em horas:', dif_horas, ', diferença em minutos: ', dif_min)


# In[13]:


#criando campos de ano, mes e dia para usar no bi

#ano
ano = df['time'].str.split('[', n = 1, expand = True)
ano = ano[1].str.split('-', n = 1, expand = True)
df['ano']= ano[0]

#mes
mes = df['time'].str.split('-', n = 1, expand = True)
mes = mes[1].str.split('-', n = 1, expand = True)
df['mes']= mes[0]

#dia
dia = mes[1].str.split('T', n = 1, expand = True)
df['dia']= dia[0]
df.head(1)


# In[14]:


#salvar o arquivo transformado em um csv para utilizar no BI
df.to_csv('apache_bi.csv', encoding='utf-8', sep=';', index=False)


# In[15]:


##########################################################
# ANÁLISER SOLICITADAS
##########################################################


# In[16]:


#01 os 5 (cinco) logins que mais efetuaram requisições;
df.groupby(['userid'])['userid'].count().sort_values(ascending=False).head(5)


# In[17]:


#02 os 3 (três) serviços que mais receberam requisições;
df.groupby(['endpoint'])['endpoint'].count().sort_values(ascending=False).head(3)


# In[18]:


#03 os 10 (dez) browsers mais utilizados;
df.groupby(['browser'])['browser'].count().sort_values(ascending=False).head(10)


# In[19]:


#04 os endereços de rede (classe C) com maior quantidade de requisições;
# nesta base, a maioria dos IPs acessou apenas uma vez, existe apenas um que acessou duas vezes mas não é classe C
df[df.classec == True].groupby(['ip'])['classec'].count().sort_values(ascending=False).head(10)


# In[20]:


#05 a hora com mais acesso no dia;
df.groupby(['hora'])['hora'].count().sort_values(ascending=False).head(5)


# In[21]:


#06 a hora com a maior quantidade de bytes;
df.groupby(['hora'])['size'].sum().sort_values(ascending=False).head(5)


# In[22]:


#07 o endpoint com maior consumo de bytes;
df.groupby(['endpoint'])['size'].sum().sort_values(ascending=False).head(5)


# In[23]:


#08 a quantidade de bytes por minuto;
print('Bytes por minuto: {:.2f}'.format(df['size'].sum()/dif_min))


# In[24]:


#09 a quantidade de bytes por hora;
print('Bytes por hora: {:.2f}'.format(df['size'].sum()/dif_horas))


# In[25]:


#10 a quantidade de usuários por minuto;
print('Quantidade de usuários por minuto: {:.2f}'.format(df['userid'].count()/dif_min))


# In[26]:


#11 a quantidade de usuários por hora;
print('Quantidade de usuários por hora: {:.2f}'.format(df['userid'].count()/dif_horas))


# In[27]:


#12 a quantidade de requisições que tiveram erro de cliente, agrupadas por erro;
df[df.status_desc == 'Erro de cliente'].groupby(['status_desc'])['status_desc'].count().sort_values(ascending=False).head(10)


# In[28]:


#13 a quantidade de requisições que tiveram sucesso;
df[df.status_desc == 'Sucesso'].groupby(['status_desc'])['status_desc'].count().sort_values(ascending=False).head(10)


# In[29]:


#14 a quantidade de requisições que foram redirecionadas;
df[df.status_desc == 'Redirecionada'].groupby(['status_desc'])['status_desc'].count().sort_values(ascending=False).head(10)


# In[30]:


####################################################################################

