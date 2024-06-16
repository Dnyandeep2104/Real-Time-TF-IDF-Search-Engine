from boto3.dynamodb.conditions import Key
from flask import Flask, render_template, request
import boto3
import termify
import math
from collections import defaultdict

#create dynamodb instances/objects for later querying
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
tbl_token_count = dynamodb.Table('tbl_token_count')
tbl_docsize = dynamodb.Table('tbl_docsize')

def retrieve_items_by_gsi_token(token):
    '''
    retrive all items using global secondary index (make sure you manually created such 
    an index on your DynamoDB table)
    example returns of 'age':
        [{'tokencount': Decimal('42'), 'docid': 'bible-kjv',      'token': 'age'}, 
         {'tokencount': Decimal('21'), 'docid': 'austen-emma',    'token': 'age'} ] 
    '''
    # Specify the GSI name
    gsi_index_name = 'token-docid-index'
    # Perform the query using the GSI. Question for you: Why GSI is needed????
    response = tbl_token_count.query(
        IndexName = gsi_index_name,
        KeyConditionExpression = Key('token').eq(token),
    )

    print('GSI query result:', token, response['Items'], '\n')
    return response['Items']

def compute_tfidf(item, token_items_dict):
    '''
    compute tfidf per item/record, noting we need 4 numbers for tfidf calculation
    item example: {'tokencount': Decimal('42'), 'docid': 'bible-kjv', 'token': 'age'} 
    token_items_dict: key is token, value is a list of docs that have this token
    '''
    print('computing tfidf for: ',item, '\n')
    # Number 1: number of times this token appears in current doc
    tokencount = int( item['tokencount'] ) 
 
    # Number 2: number of terms in a document
    total_token_in_doc = int( tbl_docsize.get_item(Key={'docid': item['docid']})['Item']['size'] )

    # Number 3: list of docs that have this token
    doc_occurrence = len(token_items_dict[item['token']])

    # Number 4: total # of docs (can avoid DB query by caching results)
    total_docs = tbl_docsize.scan(Select='COUNT')['Count']

    # tfidf = tf * idf
    tfidf = 1000000.0 * tokencount / total_token_in_doc * math.log10(total_docs / doc_occurrence)
    return tfidf

def compute_doc_relevance_per_docid(docid, docid_items_dict, token_items_dict):
    '''
    aggregate document relevance by computing tfidf for each included token
    '''
    relevance = 0.0

    # all items in items have the same docid
    # so the inner loop is looping by token
    for item in docid_items_dict[docid]:
        # since each item corresponds a token,
        # now calcuate tfidf for a token in the current doc
        tfidf = compute_tfidf(item, token_items_dict)
        relevance += tfidf
        # print('\nitem:', item, tfidf)
        
    return relevance 

def retrieve_table_data(tokens):
    '''
    retrive table records for tfidf calculation later
    this queries DynamoDB len(tokens) times
    '''
    # key is token, value is a list of docs that have this token
    token_items_dict = {}
    # key is docid, value is a list of tokens in the user query that 
    #                        appeared in the doc
    docid_items_dict = defaultdict(list)

    for token in tokens:
        # query db
        items = retrieve_items_by_gsi_token(token) 

        # prepare two dict based on query return
        token_items_dict[token] = items

        for item in items:
            docid_items_dict[item['docid']].append(item)

    return token_items_dict, docid_items_dict

app = Flask(__name__)
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/query', methods=['POST'])
def query():
    # user input processing
    line = request.form['user_input'].strip()
    tokens = termify.termify(line)

    # given sanitized user input, retrieve table data into two dicts
    token_items_dict, docid_items_dict = retrieve_table_data(tokens)

    computed_relevances = [] 
    # calcuate relevance score for each doc
    for docid in docid_items_dict.keys():
        print('docid', docid, docid_items_dict[docid])
        relevance = compute_doc_relevance_per_docid(docid, docid_items_dict, token_items_dict)
        relevance = relevance / len(tokens)
        computed_relevances.append([docid, relevance])

    # sort and return top 5
    computed_relevances.sort(key=lambda p: p[1], reverse=True)
    return computed_relevances[0:5]

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5330, debug=True)