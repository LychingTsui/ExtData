package com.qiguo.tv.movie.featuresCollection;

import java.io.File;
import java.io.IOException;

import de.bwaldvogel.liblinear.Feature;
import de.bwaldvogel.liblinear.FeatureNode;
import de.bwaldvogel.liblinear.Linear;
import de.bwaldvogel.liblinear.Model;
import de.bwaldvogel.liblinear.Parameter;
import de.bwaldvogel.liblinear.Problem;
import de.bwaldvogel.liblinear.SolverType;

public class LRPrediction {
	public static void main(String[] args) throws Exception{
		Feature[][] featuresMatrix  = new Feature[5][];
		Feature[] featureMatrix1 = {new FeatureNode(2, 0.1),new FeatureNode(3, 0.2)} ;
		Feature[] featureMatrix2 = {new FeatureNode(2, 0.1), new FeatureNode(3, 0.3), new FeatureNode(4, -1.2)};
		Feature[] featureMatrix3 = {new FeatureNode(1, 0.4)};
		Feature[] featureMatrix4 = {new FeatureNode(2, 0.1),new FeatureNode(4, 1.4), new FeatureNode(5, 0.5)};
		
		Feature[] featureMatrix5 = {new FeatureNode(1, -0.1),new FeatureNode(2, -0.2),new FeatureNode(3, 0.1),
				new FeatureNode(5, 0.1)};
		//FeatureNode fNode = new FeatureNode(2, 3);
		
		featuresMatrix[0] = featureMatrix1;
		featuresMatrix[1] = featureMatrix2;
		featuresMatrix[2] = featureMatrix3;
		featuresMatrix[3] = featureMatrix4;
		featuresMatrix[4] = featureMatrix5;
		System.out.println(featureMatrix1[1]);
		
		double[] targetVal = {1,0,1,1,0};
		Problem problem = new Problem();
		problem.l = 5;
		problem.n = 5;
		problem.x = featuresMatrix;
		problem.y = targetVal;
		
		SolverType solver = SolverType.L1R_LR;
		double c = 1.0;
		double eps = 0.001;
		Parameter parameter = new Parameter(solver, c, eps);
		Model model = Linear.train(problem, parameter);
		//File modelFile = new File("/Users/qiguo/Documents/model");
		//model.save(modelFile);	
		//model = Model.load(modelFile);
		Feature[] testNode = new FeatureNode[4];
		testNode[0] = new FeatureNode(3, 0.4);
		testNode[1] = new FeatureNode(1, 0.3);
		testNode[2] = new FeatureNode(5, 0.3);
		testNode[3] = new FeatureNode(4, 0.8);
		//double prediction = Linear.predict(model, testNode);
		double[] r = new double[1];
		Linear.predictValues(model, testNode, r);
		//System.out.print("Classification result: "+ prediction);
		System.out.println(r[0]);
		
	}
	
	
}
