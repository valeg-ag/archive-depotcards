from asyncore import read
import csv
import os
import glob
import sys


def append_create_tmp_table_stmt(sqlfile):
    sqlfile.write("""
create table TMP_MAKE_DC_ARCHIVE  (
   DCNUM  VARCHAR2(32) not null,
   WSCODE VARCHAR2(20) not null
)
storage
(
    maxextents unlimited
    pctincrease 0
)
tablespace OMP_DB;
""")


def append_drop_tmp_table_stmt(sqlfile):
    sqlfile.write("""
drop table TMP_MAKE_DC_ARCHIVE;
""")


def append_insert_into_tmp_table_stmt(sqlfile, dcnum, wscode):
    dcnum = dcnum.strip()
    wscode = wscode.strip()
    if not dcnum or not wscode:
        return

    sqlfile.write(f"""
insert into TMP_MAKE_DC_ARCHIVE(DCNUM, WSCODE)
values(\'{dcnum}\', \'{wscode}\');
""")


def append_update_dc_stmt(sqlfile):
    sqlfile.write("""
alter trigger TUA_STOCK_DEPOTCARD disable;

alter trigger TIUB_STOCK_DEPOTCARD disable;

alter trigger TUB_DEPOTCARDS disable;

alter trigger TUA_DEPOTCARDS disable;

update STOCK_DEPOTCARD D
   set ARCHIVE = 1
 where     nvl( D.ARCHIVE, 0 ) = 0
       and exists( select 1 from TMP_MAKE_DC_ARCHIVE T
                    where     T.DCNUM = D.DEPOTCARDNUM
                          and exists( select 1 from DIVISIONOBJ DOBJ
                                       where     DOBJ.CODE = D.WSCODE      
                                             and DOBJ.WSCODE = T.WSCODE
                                             and DOBJ.DIVISION_TYPE = 105 ) );

update DEPOTCARDS D
   set ARCHIVE = 1
 where     nvl( D.ARCHIVE, 0 ) = 0
       and D.BASETYPE = 1
       and exists( select 1 from TMP_MAKE_DC_ARCHIVE T
                    where     T.DCNUM = D.DEPOTCARDNUM
                          and exists( select 1 from DIVISIONOBJ DOBJ
                                       where     DOBJ.CODE = D.WSCODE      
                                             and DOBJ.WSCODE = T.WSCODE
                                             and DOBJ.DIVISION_TYPE = 105 ) );

alter trigger TUA_STOCK_DEPOTCARD enable;

alter trigger TIUB_STOCK_DEPOTCARD enable;

alter trigger TUB_DEPOTCARDS enable;

alter trigger TUA_DEPOTCARDS enable;
""")


def skip_lines_in_csv(reader, n):
    for i in range(n):
        next(reader)


if __name__ == '__main__':

    if not os.path.exists("./data"):
        print("Couldn't find \"./data\" directory. Create \"./data\" directory and put csv-files into it", file=sys.stderr)
        sys.exit(1)

    csvfiles = [os.path.splitext(os.path.basename(f))[0]
                for f in glob.glob("./data/*.csv")]

    if not len(csvfiles):
        print("Couldn't find csv-files in \"./data\" directory. Put csv-files into \"./data\" directory", file=sys.stderr)
        sys.exit(1)

    for filename in csvfiles:
        with open(f"./data/{filename}.sql", "w") as sqlfile:

            append_create_tmp_table_stmt(sqlfile)

            with open(f"./data/{filename}.csv") as csvfile:
                reader = csv.reader(csvfile, delimiter=';')
                skip_lines_in_csv(reader, 2)
                for row in reader:
                    append_insert_into_tmp_table_stmt(sqlfile, row[1], row[2])

            append_update_dc_stmt(sqlfile)

            append_drop_tmp_table_stmt(sqlfile)
