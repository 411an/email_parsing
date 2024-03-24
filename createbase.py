import sqlite3

conn = sqlite3.connect('example.db')

cur = conn.cursor()

create_table1 = '''
CREATE TABLE "mainmails" (
	"id" INTEGER NOT NULL,
	"sender" TEXT NULL,
	"subject" TEXT NULL,
	"body" BLOB NULL,
	"messageid" TEXT NULL,
	"date" DATETIME NULL,
	PRIMARY KEY ("id")
)
;
'''

create_table2 = '''
CREATE TABLE "jobmails" (
	"messageid" VARCHAR(255) NULL,
	"date" DATE NULL,
	"sender" VARCHAR(255) NULL,
	"msg" TEXT NULL,
	"subject" VARCHAR(255) NULL
)
;

'''

create_table3 = '''
CREATE TABLE "all_mails" (
	"messageid" VARCHAR(255) NULL,
	"model" VARCHAR(255) NULL,
	"cancel" REAL NULL,
	"apply" REAL NULL,
	"other" REAL NULL
)
;
'''

create_table4 = '''
CREATE TABLE "mailinfo" (
	"messageid" VARCHAR(255) NULL,
	"category" CHAR(5) NULL,
	"date" DATETIME NULL,
	"company" VARCHAR(255) NULL,
	"jobtitle" VARCHAR(255) NULL,
	"msg" TEXT NULL,
	"company_group" VARCHAR(255) NULL
)
;
'''

cur.execute(create_table1)
cur.execute(create_table2)
cur.execute(create_table3)
cur.execute(create_table4)

conn.commit()

conn.close()
