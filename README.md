# Desenvolvimento da aplicação Python



## Instalação de software

Precisa de ter o Python 3 e o gestor de pacotes pip instalados.
Experimente executar `python3 --version` e `pip3 --version` para saber
se já estão instalados. Em caso negativo, pode por exemplo, em Ubuntu,
executar:

```
sudo apt-get install python3 python3-pip
```

Tendo Python 3 e pip instalados, deve instalar a biblioteca `Flask` executando o comando:

```
pip3 install --user Flask
``` 

## Configuração de acesso à BD

Edite o ficheiro `db.py` no que se refere à configuração da sua BD, modificando o parâmetro `DB_FILE`, que indica o ficheiro da base de dados. Esse ficheiro deve residir na mesma pasta que o ficheiro `app.py`.

Configurado o parâmetro `DB_FILE`,  teste o acesso executando:

```
python3 test_db_connection.py NOME_DE_UMA_TABELA
```

Se a configuração do acesso à BD estiver correto, deverá ser listado o conteúdo da tabela `NOME_DE_UMA_TABELA`. Por exemplo, se a BD configurada fosse a da bilheteira (Ficha 5) e   quiséssemos listar a tabela `artistas`:

```
$ python3 test_db_connection.py artistas
10 results ...
[('NIF', 203304125), ('NOME', 'Pedro Burmester'), ('D_NASCE', '1968-02-23'), ('TIPO', 'pianista')]
[('NIF', 203608991), ('NOME', 'Jorge Palma'), ('D_NASCE', '1960-01-11'), ('TIPO', 'pianista')]
[('NIF', 204331998), ('NOME', 'Jose Lobo'), ('D_NASCE', '1955-12-03'), ('TIPO', 'maestro')]
[('NIF', 204783229), ('NOME', 'Sergio Godinho'), ('D_NASCE', '1945-04-25'), ('TIPO', 'cantor')]
[('NIF', 204949576), ('NOME', 'Joan Baez'), ('D_NASCE', '1944-08-15'), ('TIPO', 'cantor')]
[('NIF', 205843223), ('NOME', 'Miguel Santos'), ('D_NASCE', '1980-03-26'), ('TIPO', 'musico')]
[('NIF', 205923490), ('NOME', 'Ana Bacalhau'), ('D_NASCE', '1985-12-13'), ('TIPO', 'cantor')]
[('NIF', 207659130), ('NOME', 'Luis Costa'), ('D_NASCE', '1980-11-03'), ('TIPO', 'maestro')]
[('NIF', 208485301), ('NOME', 'Nuno Medeiros'), ('D_NASCE', '1979-04-11'), ('TIPO', 'cantor')]
[('NIF', 209884332), ('NOME', 'Joana Pereira'), ('D_NASCE', '1990-09-28'), ('TIPO', 'pianista')]
```

## Execução do servidor da aplicação

Depois de configurar a BD como descrito acima, pode agora iniciar o servidor da aplicação executando `python3 server.py`:

```
$ python3 server.py
2021-05-18 21:40:46 - INFO - Connected to database guest
 * Serving Flask app "app" (lazy loading)
 * Environment: production
   WARNING: This is a development server.  Do not use it in a production deployment.
   Use a production WSGI server instead.
 * Debug mode: off
2021-12-08 21:40:46 - INFO -  * Running on http://0.0.0.0:9000/ (Press CTRL+C to quit) 
...
```

De seguida, abra no seu browser __http://127.0.0.1:9000__ ou __http://localhost:9000__. Deverá ver uma página com uma mensagem __Hello World!__, da forma ilustrada na imagem a seguir.

![](static/app_screenshot.png)

## Programação

A estrutura deverá ser similar à da aplicação MovieStreamApp que será abordada nas aulas teóricas. Pode consultar a propósito [slides e código no GitHub](https://github.com/edrdo/MovieStreamApp).

Deve editar o código Python da aplicação em `app.py`. Cada "endpoint" da aplicação  deve efetuar uma ou mais interrogações à base de dados e utilizar os dados obtidos para gerar HTML usando templates Jinja.
Deve colocar os templates de geração de HTML (uma por "endpoint") na pasta `templates`.

### Exemplos na MovieStreamApp 


Informação de um filme - "endpoint" `/movies/int:id`:

- [Código no método `get_movie` em app.py](https://github.com/edrdo/MovieStreamApp/blob/master/app.py#L46)
- [Template em `templates/movie.html`](https://github.com/edrdo/MovieStreamApp/blob/master/templates/movie.html)

### Sumário das principais tags usadas no código da MovieStreamApp

#### Jinja

- `{{ x.attr }}` : expande para valor de atributo  `attr` para variável `x` -  [[ver documentação]](https://jinja.palletsprojects.com/en/3.0.x/templates/#variables) 
- `{% for x in items %} ... {% endfor %}`: iteração `for` sobre lista de valores `items` [[ver documentação]](https://jinja.palletsprojects.com/en/3.0.x/templates/#for)


#### HTML (com apontadores para tutorial W3Schools)

- `<a href ...>`: [links](https://www.w3schools.com/html/html_links.asp)
- `<table> <th> <tr> <td>`: [formatação de tabelas](https://www.w3schools.com/html/html_tables.asp)
- `<ul>`, `<ol>` `<li>`: [formatação de listas](https://www.w3schools.com/html/html_lists.asp)
- `<h1>, <h2>, ...`: [cabeçalhos de nível 1, 2, ...](https://www.w3schools.com/html/html_headings.asp)
- `<p>`: [parágrafos](https://www.w3schools.com/html/html_paragraphs.asp)
- `<b>, <i>, ...`: [formatação de texto em negrito, itálico, ...](https://www.w3schools.com/html/html_formatting.asp)


## Mais referências

- HTML: 
   - [W3Schools - tutorial simples](https://www.w3schools.com/html/default.asp)
   - [referência da Mozilla](https://developer.mozilla.org/en-US/docs/Web/HTML) 
- Bibliotecas:
  - [sqlite3](https://docs.python.org/3/library/sqlite3.html)
  - [Flask](https://flask.palletsprojects.com/en/1.1.x/)
  - [Jinja templates](https://jinja.palletsprojects.com/en/2.10.x/templates/)

