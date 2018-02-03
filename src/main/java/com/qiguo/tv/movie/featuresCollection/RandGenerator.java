package com.qiguo.tv.movie.featuresCollection;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.hdfs.server.datanode.dataNodeHome_jsp;

import com.qiguo.tv.movie.featuresCollection.Roc_AUC;

import de.bwaldvogel.liblinear.Feature;
import de.bwaldvogel.liblinear.FeatureNode;
import de.bwaldvogel.liblinear.Linear;
import de.bwaldvogel.liblinear.Model;
import de.bwaldvogel.liblinear.Parameter;
import de.bwaldvogel.liblinear.Problem;
import de.bwaldvogel.liblinear.SolverType;

public class RandGenerator {
	public static final int Num = 300000;
	public static Feature[][] randGenerator(){
		java.util.Random r = new Random();
		Feature[][] featureMatrix = new Feature[Num][];
		for(int i=0; i < Num; i++){
			Feature[] features = new FeatureNode[100];
			for(int j=0; j<100; j++){
				features[j] = new FeatureNode(j+1, r.nextDouble()); // featureNode的索引值从1开始
			}
			featureMatrix[i] = features;
		}
		return featureMatrix;
	}

	
	public static void main(String[] args) throws IOException{
		Feature[][] featureMtr = randGenerator();
		double[] target = new double[Num];
		for(int idx = 0; idx < Num; idx++){
			java.util.Random ran = new Random();
			if(ran.nextDouble() > 0.5){
				target[idx] = 1;
			}
			else{
				target[idx] = 0;
			}
		}
		Problem problem = new Problem();
		problem.l = 300000;
		problem.n = 100;
		problem.x = featureMtr;
		problem.y = target;
		
		SolverType solver1 = SolverType.L2R_LR;
		//SolverType solver2 = SolverType.L1R_LR;
		double c = 1.0;
		double eps = 0.001;
		Parameter parameter1 = new Parameter(solver1, c, eps);
		//Parameter parameter2 = new Parameter(solver2, c, eps);
		Model model = Linear.train(problem, parameter1);
		//Model model2 = Linear.train(problem, parameter2);
		
		//File modelFile = new File("/Users/qiguo/Documents/model1");
		//File modelFile2 = new File("/Users/qiguo/Documents/model2");
		//model.save(modelFile);
		//model2.save(modelFile2);
		
		//model = Model.load(modelFile);
		//model2 = Model.load(modelFile2);
		double[] pred = new double[problem.l];
		Feature[] testNode = new  FeatureNode[100];
		for(int i=0;i<100;i++){
			testNode[i] = new FeatureNode(i+1, 0.8) ;
		}
		//double res = Linear.predict(model2, testNode);
	
		for(int i=0; i<problem.l;i++){
			pred[i] = Linear.predict(model, problem.x[i]);
		
		}
		double[] r1 = new double[1];
		//double rs = Linear.predict(model2, testNode);
		Linear.predictValues(model, testNode, r1);
		System.out.println(r1[0]);
		//System.out.println(r1[1]);
		//System.out.println(rs);
		//Linear.predictValues(model2, testNode, r1);
		//double prediction = Linear.predict(model, testNode);
		//System.out.print("Classification result: "+ prediction);
		double auc = new Roc_AUC(pred, problem.y).CalculateAUC();
		System.out.println("auc: "+ auc );	
		
	}
	
}
