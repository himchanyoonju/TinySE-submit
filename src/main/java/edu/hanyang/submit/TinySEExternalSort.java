package edu.hanyang.submit;

import java.io.*;
import java.util.*;

import edu.hanyang.indexer.ExternalSort;
import org.apache.commons.lang3.tuple.MutableTriple;

public class TinySEExternalSort implements ExternalSort {
	int blocksize;
	int nblocks;
	int tupleSize;
	int runSize;
	String infile;
	String outfile;
	String tmpdir;

	public void sort(String infile, String outfile, String tmpdir, int blocksize, int nblocks) throws IOException {
		this.blocksize = blocksize;
		this.nblocks = nblocks;
		this.infile = infile;
		this.outfile = outfile;
		this.tmpdir = tmpdir;
		this.tupleSize = blocksize / ((Integer.SIZE/Byte.SIZE) * 3);
//		this.runSize = tupleSize * nblocks;
		this.runSize = blocksize * nblocks / Integer.SIZE / 3;
		initialRun();
		externalMergeSort(1);

	}



	private void initialRun()  throws  IOException {
		File output = new File(this.outfile);
		if (output.exists()) {
			output.delete();
		}
		File dir = new File(this.tmpdir);
		if (!dir.exists()) {
			dir.mkdirs();
		} else {
			dir.delete();
			dir.mkdirs();
		}



		final ArrayList<MutableTriple<Integer, Integer, Integer>> triples = new ArrayList<>(runSize);
		final DataManager dataManager = new DataManager(new DataInputStream(new BufferedInputStream(new FileInputStream(infile),blocksize)));
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
			for(int run = 0; run < runSize; run++) {
				final MutableTriple<Integer, Integer, Integer> tuple = new MutableTriple<>();
				if (dataManager.getTuple(tuple)) {
					triples.add(tuple);
				}
			}
			Collections.sort(triples);
			final String fileName = dir.getAbsoluteFile()+File.separator+run_cnt;
			final DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fileName)));
			for (MutableTriple<Integer, Integer, Integer> data : triples) {
				dos.writeInt(data.getLeft());
				dos.writeInt(data.getMiddle());
				dos.writeInt(data.getRight());
			}
			dos.flush();
			dos.close();;
			run_cnt++;
			triples.clear();
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
			n_way_merge(dataManagers, this.outfile);
			for (DataManager dataManager : dataManagers) {
				dataManager.close();
			}
			return;
		}
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

	private void n_way_merge(ArrayList<DataManager> dataManagers, String outputFile) throws IOException {
		final MutableTriple<Integer, Integer, Integer> tuple = new MutableTriple<>();
		final DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile), blocksize));
		final PriorityQueue<DataManager> queue = new PriorityQueue<>(dataManagers.size(), new Comparator<DataManager>() {
			public int compare(DataManager o1, DataManager o2) {
				return o1.tuple.compareTo(o2.tuple);
			}
		});
		queue.addAll(dataManagers);
		while (queue.size() != 0){
			final DataManager dataManager = queue.poll();
			if (dataManager.getTuple(tuple)) {
				dos.writeInt(tuple.getLeft());
				dos.writeInt(tuple.getMiddle());
				dos.writeInt(tuple.getRight());
				queue.add(dataManager);
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
					"/Users/himchanyoon/Desktop/TinySE-submit-master3/src/test/resources/tmp",
					4096,
					1000);
			System.out.println(System.currentTimeMillis()-start+" msecs");
		}
		catch(Exception e){
			e.printStackTrace();;
			System.out.println(e);
		}
	}

}

class DataManager {
	public boolean isEOF = false;
	private DataInputStream dis = null;
	public MutableTriple<Integer, Integer, Integer> tuple = new MutableTriple<Integer, Integer, Integer>(0,0,0);
	public DataManager(DataInputStream dis) throws IOException {
		this.dis = dis;
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

	public boolean readNext() throws IOException {
		if(hasNext() == false) {
			return false;
		}
		tuple.setLeft(dis.readInt());
		tuple.setMiddle(dis.readInt());
		tuple.setRight(dis.readInt());
		return true;
	}

	public boolean getTuple(MutableTriple<Integer, Integer, Integer> ret) throws IOException {
		if (hasNext() == false) {
			return false;
		}
		isEOF = (!readNext());
		ret.setLeft(tuple.getLeft());
		ret.setMiddle(tuple.getMiddle());
		ret.setRight(tuple.getRight());
		return true;
	}




	public void close() throws IOException {
		this.dis.close();
	}
}