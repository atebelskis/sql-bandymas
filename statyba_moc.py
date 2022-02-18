from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError, PendingRollbackError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship
import csv

user = 'root'
password = '1My_sql!SQL'

eng = create_engine('mysql://'+user+':'+password+'@localhost:3306/statyba')

baze = declarative_base()

class Objektai(baze):
    __tablename__ = 'objektai'

    obj_id = Column(Integer, primary_key=True, autoincrement=True)
    tipas_pavad = Column(String(100), nullable=False)
    adresas = Column(String(100), nullable=False)
    #sutartis = relationship('Sutartys', back_populates='objektas')


class Uzsakovai(baze):
    __tablename__ = 'uzsakovai'

    uzs_id= Column(Integer, primary_key=True, autoincrement=True)
    pavadinimas = Column(String (100), nullable=False)
    imones_k = Column(Integer)
    #sutartis = relationship('Sutartys', back_populates='uzsakovas')


class Sutartys(baze):
    __tablename__ = 'sutartys'

    sut_id = Column(Integer, primary_key=True, autoincrement=True)
    sut_nr = Column(String(30), nullable=False)
    uzs_id = Column(Integer, ForeignKey('uzsakovai.uzs_id', ondelete='CASCADE'), nullable=False)
    obj_id = Column(Integer, ForeignKey('objektai.obj_id', ondelete='CASCADE'),nullable=False)
    sut_suma = Column(Integer)
    #uzsakovas = relationship('Uzsakovai', back_populates='sutartis')
    #objektas = relationship('Objektai', back_populates='sutartis')





try:

    objektai =  [
        {'tipas_pavad': 'DGN', 'adresas': 'Sodu g.1, Vilnius'}]

    uzsakovai = [
        {'pavadinimas': 'UAB RK', 'imones_k': 11111234 }]

    sutartys = [
        {'sut_nr': '201', 'uzs_id': 1, 'obj_id': 1, 'sut_suma': 20000}
    ]

    data_file = csv.DictReader(open("./statyba.csv", mode='r+t'))
    data_from_dict = csv.DictReader(data_file)

    baze.metadata.create_all(eng)
    Session = sessionmaker(bind=eng)
    session = Session()


    def insert_(session, baze, params):
        session.add(baze(**params))
        session.commit()

    for i in objektai:
        insert_(session, Objektai, i)

    for i in uzsakovai:
        insert_(session, Uzsakovai, i)

    for i in sutartys:
        insert_(session, Sutartys, i)



    print('DB ok')


except Exception as e:
    print(e)

for row in data_file:
    objektai_file = Objektai(tipas_pavad=row['tipas_pavad'], adresas=row['adresas'])
    session.add(objektai_file)
    session.commit()
