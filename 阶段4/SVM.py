from pyspark import SparkContext
from pyspark.mllib.classification import SVMWithSGD, SVMModel
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.evaluation import MulticlassMetrics,BinaryClassificationMetrics

sc = SparkContext(appName="stage4_svm")
# Load and parse the data
def parsePoint(line):
    values = [float(x) for x in line.split(',')]
    return LabeledPoint(values[4], values[0:3])

data = sc.textFile("train_after.csv")
parsedData = data.map(parsePoint)
# split train and test
train_data,test_data=parsedData.randomSplit([0.7,0.3])
# Build the model
model = SVMWithSGD.train(parsedData, iterations=100)

# Evaluating the model on training data
labelsAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
trainErr = labelsAndPreds.filter(lambda lp: lp[0] != lp[1]).count() / float(parsedData.count())
Accuracy = 1-trainErr
Recall = labelsAndPreds.filter(lambda lp: lp[0] == lp[1]).filter(lambda lp: lp[0] == 1).count() / float(labelsAndPreds.filter(lambda lp: lp[0] == 1).count())
print("\n")
#展示前20条预测结果
predict=model.predict(test_data.map(lambda p:p.features))
predict_all=predict.zip(test_data.map(lambda p:p.features))
for i in range(0,20):
    print(predict_all.collect()[i])

#评估模型的预测准确率和其他指标
print("\n")
print("Accuracy = " + str(Accuracy))#准确率
print("Recall = " + str(Recall))#召回率

