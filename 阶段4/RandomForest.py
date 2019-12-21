from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils
from pyspark import SparkContext

sc = SparkContext(appName="stage4_forest")
# Load and parse the data file into an RDD of LabeledPoint.
data = MLUtils.loadLibSVMFile(sc,'train_after_libsvm.txt')
# Split the data into training and test sets (30% held out for testing)
(train_data, test_data) = data.randomSplit([0.7, 0.3])

# Train a RandomForest model.
#  Empty categoricalFeaturesInfo indicates all features are continuous.
#  Note: Use larger numTrees in practice.
#  Setting featureSubsetStrategy="auto" lets the algorithm choose.
model = RandomForest.trainClassifier(train_data, numClasses=2, categoricalFeaturesInfo={},
                                     numTrees=3, featureSubsetStrategy="auto",
                                     impurity='gini', maxDepth=4, maxBins=32)

# Evaluate model on test instances and compute test error
predictions = model.predict(test_data.map(lambda x: x.features))
labelsAndPredictions = test_data.map(lambda lp: lp.label).zip(predictions)
testErr = labelsAndPredictions.filter(
    lambda lp: lp[0] != lp[1]).count() / float(test_data.count())
Accuracy = 1-testErr
#Recall = train_data.filter(lambda lp: lp[0] == lp[1]).filter(lambda lp: lp[0] == 1).count() / float(train_data.filter(lambda lp: lp[0] == 1).count())
print("\n")
#展示前20条预测结果
predict=model.predict(test_data.map(lambda p:p.features))
predict_all=predict.zip(test_data.map(lambda p:p.features))
for i in range(0,20):
    print(predict_all.collect()[i])

#评估模型的预测准确率和其他指标
print("\n")
print("Accuracy = " + str(Accuracy))#准确率
#print("Recall = " + str(Recall))#召回率