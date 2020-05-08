package edu.hanyang.submit;

import java.io.*;
import java.sql.Struct;
import java.util.*;

import edu.hanyang.indexer.ExternalSort;
import org.apache.commons.lang3.tuple.MutableTriple;

import javax.xml.crypto.Data;

// 선언
public class TinySEExternalSort implements ExternalSort {
	int blocksize;
	int nblocks;
	int tupleSize;
	int runSize;
	String infile;
	String outfile;
	String tmpdir;

	//Sort 시작
	public void sort(String infile, String outfile, String tmpdir, int blocksize, int nblocks) throws IOException {
		this.blocksize = blocksize;
		this.nblocks = nblocks;
		this.infile = infile;
		this.outfile = outfile;
		this.tmpdir = tmpdir;
		this.tupleSize = blocksize / ((Integer.SIZE/Byte.SIZE) * 3);
		this.runSize = tupleSize * nblocks;
		//	처음에 데이터 쪼개고
		initialRun();
//		데이터 합치기 merge
		externalMergeSort(1);
//		display();
	}

	// 파일을 잘라서 분리하면서 정렬하면서 저장
	private void initialRun()  throws  IOException {
		File dir = new File(this.tmpdir);
		if (!dir.exists()) {
			dir.mkdirs();
		} else {
			dir.delete();
			dir.mkdirs();
		}

		ArrayList<MutableTriple<Integer, Integer, Integer>> triples = new ArrayList<>(runSize);
		DataManager dataManager = new DataManager(new DataInputStream(new BufferedInputStream(new FileInputStream(infile),blocksize)));
		int run_cnt = 0;
		int step = 0;
		dir = new File(this.tmpdir+step);
		if (!dir.exists()) {
			dir.mkdir();
		} else {
			dir.delete();
			dir.mkdir();
		}
		while(dataManager.hasNext()) {
			// runsize 한번에 메모리 처리
			for(int run = 0; run < runSize; run++) {
				MutableTriple<Integer, Integer, Integer> tuple = dataManager.getTuple();
				if (tuple != null) {
					triples.add(tuple);
				}
			}
//			API 사용
			Collections.sort(triples);
			final String fileName = dir.getAbsoluteFile()+File.separator+run_cnt;
			DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fileName)));
//			초기 run 생성
//			temp 파일 하나씩 씀
			for (MutableTriple<Integer, Integer, Integer> data : triples) {
				dos.writeInt(data.getLeft());
				dos.writeInt(data.getMiddle());
				dos.writeInt(data.getRight());
			}
			dos.flush();
			dos.close();;
			run_cnt++;
			triples.clear();
			triples = new ArrayList<>(runSize);
		}
		dataManager.close();
	}


	private void externalMergeSort(int step) throws  IOException {
		int preStep = step-1;
		File[] files = (new File(this.tmpdir+String.valueOf(preStep))).listFiles();
		ArrayList<DataManager> dataManagers = new ArrayList();
		if(files.length <= nblocks - 1) {
			for (File f : files) {
				DataInputStream input = new DataInputStream(new BufferedInputStream(new FileInputStream(f), blocksize));
				dataManagers.add(new DataManager(input));
			}
//			N way merge 정렬
			n_way_merge(dataManagers, this.outfile);
			for (DataManager dataManager : dataManagers) {
				dataManager.close();
			}
			return;
		}
//		파일 크기가 클 때
		File file = new File(this.tmpdir+step);
		if (file.exists() == false) {
			file.mkdirs();
		} else {
			file.delete();
			file.mkdirs();
		}
		int run_cnt = 0;
		int cnt = 0;
		for (File f : files) {
			cnt++;
			dataManagers.add(new DataManager(new DataInputStream(new BufferedInputStream(new FileInputStream(f), blocksize))));
			if (cnt == this.nblocks - 1) {
				n_way_merge(dataManagers, this.tmpdir+step+File.separator+run_cnt);
				for (DataManager dataManager : dataManagers) {
					dataManager.close();
				}
				dataManagers.clear();
				run_cnt++;
				cnt = 0;
			}
		}
		if (dataManagers.isEmpty() == false) {
			n_way_merge(dataManagers, this.tmpdir+step+File.separator+run_cnt);
			for (DataManager dataManager : dataManagers) {
				dataManager.close();
			}
			dataManagers.clear();
		}
		externalMergeSort(++step);
	}

//	merge 일부

	private void n_way_merge(ArrayList<DataManager> dataManagers, String outputFile) throws IOException {
		final DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile), blocksize));
		while (true) {
			final ArrayList<MutableTriple<Integer, Integer, Integer>> triples = new ArrayList<>(runSize);
			for(int run = 0; run < runSize;) {
				for (DataManager dataManager : dataManagers) {
					final MutableTriple<Integer, Integer, Integer> tuple = dataManager.getTuple();
					if (tuple != null) {
						triples.add(tuple);
					}
					run++;
				}
			}
			if (triples.isEmpty()) break;
			Collections.sort(triples);
			for (final MutableTriple<Integer, Integer, Integer> tuple : triples) {
				dos.writeInt(tuple.getLeft());
				dos.writeInt(tuple.getMiddle());
				dos.writeInt(tuple.getRight());
			}
		}
		dos.flush();
		dos.close();
	}


	public static void main(String[] args){
		try{
			long start = System.currentTimeMillis();
			TinySEExternalSort ts = new TinySEExternalSort();
			ts.sort(
					"/Users/himchanyoon/Desktop/TinySE-submit-master3/src/test/resources/test.data",
					"/Users/himchanyoon/Desktop/TinySE-submit-master3/src/test/resources/output.data",
					"/Users/himchanyoon/Desktop/TinySE-submit-master3/src/test/resources/tmp/",
					512,
					64);
			System.out.println(System.currentTimeMillis()-start+" msecs");
		}
		catch(Exception e){
			e.printStackTrace();;
			System.out.println(e);
		}
	}

}

//파일입출력
class DataManager {
	static boolean HasNext(ArrayList<DataManager> dataManagers) throws IOException {
		for (DataManager dataManager : dataManagers) {
			if (dataManager.hasNext()) {
				return true;
			}
		}
		return false;
	}
	public boolean isEOF = false;
	private DataInputStream dis = null;
	private int position = 0;
	public MutableTriple<Integer, Integer, Integer> tuple = new MutableTriple<Integer, Integer, Integer>(0,0,0);
	public DataManager(DataInputStream dis) throws IOException {
		this.dis = dis;
	}

	public int getPosition() {
		return position;
	}

	public boolean hasNext() throws IOException {
		if (isEOF) {
			return false;
		}
		if (this.dis.available() < 1) {
			this.isEOF = true;
			return false;
		}
		return true;
	}

	private boolean readNext() throws IOException {
		if(hasNext() == false) {
			return false;
		}
		tuple.setLeft(dis.readInt());
		tuple.setMiddle(dis.readInt());
		tuple.setRight(dis.readInt());
		position++;
		return true;
	}

	public MutableTriple<Integer,Integer, Integer> getTuple() throws IOException {
		if (hasNext() == false) {
			return null;
		}
		isEOF = (!readNext());
		MutableTriple<Integer,Integer, Integer> ret = new MutableTriple<Integer, Integer, Integer>();
		ret.setLeft(tuple.getLeft());
		ret.setMiddle(tuple.getMiddle());
		ret.setRight(tuple.getRight());
		return ret;
	}

	public void close() throws IOException {
		this.dis.close();
	}
}