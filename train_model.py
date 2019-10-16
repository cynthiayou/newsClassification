from sklearn.model_selection import train_test_split
import pandas as pd
from sklearn.feature_extraction.text import TfidfTransformer, CountVectorizer, ENGLISH_STOP_WORDS, TfidfVectorizer
from sklearn.linear_model import LogisticRegression, SGDClassifier
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline
from sklearn.metrics import accuracy_score, roc_auc_score, classification_report, confusion_matrix
from sklearn.model_selection import train_test_split
from sklearn.externals import joblib
import string
import os
import shutil
import sys


#from sklearn.feature_extraction.text import TfidfVectorizer
#from sklearn.linear_model.logistic import LogisticRegression
#from sklearn.metrics import roc_auc_score, accuracy_score, mean_squared_error, confusion_matrix

def readFile(file):
    print("reading " + file + "......")

    #df = pd.read_csv(file, index_col=False)

    df = pd.read_csv(file, header=0, encoding='utf-8')
    #print("df from file: ", df.head())
    df['label'] = df['label'].astype(int)
    #print("df type: ", df.dtypes)


    return df


def preProcess(train):

    print("begin pre-processing...")

    return train

def logisticreg_train(X_train, X_test, y_train, y_test, tfidf_vec):

    print("begin logistic regression train...")

    clf = Pipeline([('tfidfvec', tfidf_vec),
                    ('logistic regression', LogisticRegression(solver='lbfgs',
                                                               C=10,
                                                               multi_class='multinomial',
                                                               max_iter=1000))])
    #print("X_train:\n", X_train)
    clf_fit = clf.fit(X_train, y_train)

    y_pred = clf_fit.predict(X_test)

    comp = pd.DataFrame({"target":y_test, "prediction":y_pred})
    #print("target and prediction:\n", comp)

    print("validation data accuracy: ", accuracy_score(y_test, y_pred))

    # save model
    model_path = "lr_model"

    if os.path.exists(model_path):
        #shutil.rmtree(model_path)
        os.remove(model_path)
    joblib.dump(clf_fit, model_path)

    new_clf = joblib.load(model_path)
    y_train_pred = clf_fit.predict(X_train)
    print("predict tain data accuracy: ", accuracy_score(y_train, y_train_pred))

    return

def MultinomialNB_train(X_train, X_test, y_train, y_test, tfidf_vec):

    print("begin MultinomialNB_train...")

    clf = Pipeline([('tfidfvec', tfidf_vec),
                    ('navie bayes classifier', MultinomialNB())])
    #print("X_train:\n", X_train)
    clf_fit = clf.fit(X_train, y_train)

    y_pred = clf_fit.predict(X_test)

    comp = pd.DataFrame({"target":y_test, "prediction":y_pred})
    #print("target and prediction:\n", comp)

    print("validation data accuracy: ", accuracy_score(y_test, y_pred))

    # save model
    model_path = "nb_model"

    if os.path.exists(model_path):
        #shutil.rmtree(model_path)
        os.remove(model_path)
    joblib.dump(clf_fit, model_path)

    new_clf = joblib.load(model_path)
    y_train_pred = clf_fit.predict(X_train)
    print("predict tain data accuracy: ", accuracy_score(y_train, y_train_pred))

    return

def SGD_train(X_train, X_test, y_train, y_test, tfidf_vec):

    print("begin SGD_train....")

    sgd_clf = SGDClassifier(loss='hinge',
                            penalty='l2',
                            alpha=1e-3,
                            learning_rate= 'adaptive',
                            eta0= 0.1,
                            max_iter=50,
                            tol = 1e-3,
                            random_state=42)
    clf = Pipeline([('tfidfvec', tfidf_vec),
                    ('SGD', sgd_clf)])
    #print("X_train:\n", X_train)
    clf_fit = clf.fit(X_train, y_train)

    y_pred = clf_fit.predict(X_test)

    comp = pd.DataFrame({"target":y_test, "prediction":y_pred})
    #print("target and prediction:\n", comp)

    print("validation data accuracy: ", accuracy_score(y_test, y_pred))

    # save model
    model_path = "sgd_model"
    if os.path.exists(model_path):
        #shutil.rmtree(model_path)
        os.remove(model_path)
    joblib.dump(clf_fit, model_path)

    new_clf = joblib.load(model_path)
    y_train_pred = clf_fit.predict(X_train)
    print("predict tain data accuracy: ", accuracy_score(y_train, y_train_pred))

def trainModel(train):

    nrows = train.shape[0]
    print("nrows: ", nrows)

    mystplst = {"-", "'"}
    stpwdlst = ENGLISH_STOP_WORDS.union(set(string.punctuation).union(mystplst))
    #print("stop words list: ", stpwdlst)

    nrows, ncols = train.shape[0], train.shape[1]
    X = train['text']
    y = train['label']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=101)
    #print("X_train: ", X_train)
    print("training data count: ", X_train.shape[0])
    print("test data count: ", X_test.shape[0])

    tfidf_vec = TfidfVectorizer(min_df=3,
                                stop_words='english',
                                strip_accents='unicode',
                                analyzer='word')
    tfidf_trans = tfidf_vec.fit_transform(train['text'])
    #print("after tfdidf transforme: ", tfidf_trans)
    #print("tfidf_feature names: ", tfidf_vec.get_feature_names())
    #print("tfidf_vocabulary: ", tfidf_vec.vocabulary_)

    if method =='0':
        MultinomialNB_train(X_train, X_test, y_train, y_test, tfidf_vec)
    elif method == '1':
        SGD_train(X_train, X_test, y_train, y_test, tfidf_vec)
    elif method == '2':
        logisticreg_train(X_train, X_test, y_train, y_test, tfidf_vec)




    return


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print('Usage hw3_train_model.py <train.csv> <classify method>')
        print("classify method: 0-MultinomialNB_train; 1-SGD_train; 2-LogisticRegression")
        exit()


    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    file = sys.argv[1]
    global method
    method = sys.argv[2]

    dir, filename = os.path.split(os.path.abspath(__file__))
    #file = "file://" + dir + "/" + sys.argv[1]
    print("input file: " + file)


    global train
    train = readFile(file)

    preProcess(train)

    trainModel(train)
