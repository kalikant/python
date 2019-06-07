'''
Create your file requirements.txt and add below dependecy

flask
flask_sqlalchemy
sqlalchemy

pip install -i -r requirements.txt
'''

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
import random


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///newsDB.sqlite3'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

class News(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    news = db.Column(db.String(300))

    def __repr__(self):
        return self.id + " - " + self.news

    def _string(self):
        return str(self.news)

@app.route("/", methods=['GET'])
def home():
    return "Hello Python!"

@app.route("/tellmeanews", methods=['GET'])
def tellmeanews():

    max_number = db.session.query(func.max(News.id)).scalar()
	News_id = random.randint(0, max_number)
	random_News_query_object = News.query.filter_by(id=News_id)
	random_News_list = random_News_query_object.all()
	random_News_object = random_News_list[0]
	random_News = random_News_object.News

    return random_News
  
@app.route("/initialize", methods=['GET'])
def initialize():
    db.create_all()
	counter = 0
    with open("News.txt")as input_file:
        for line in input_file:
            News = News()
            News.id = counter
            News.News = line
			db.session.add(News)
            db.session.commit()
            counter = counter + 1
            print(line + "added to DB")

        db.session.flush()

    return "database created successfully"

if __name__ == "__main__":
    app.run(debug=True, host='localhost', port=8080)        
   
