public class ShortestPath {

	public DenseMatrixInt extendShortestPaths(DenseMatrixInt A) {
		int n = A.width;
		DenseMatrixInt LL = new DenseMatrixInt(n, n);
		LL.fill(Integer.MAX_VALUE);

		for (int i = 0; i < n; i++) {
			System.out.println(i);
			for (int j = 0; j < n; j++) {
				for (int k = 0; k < n; k++) {
					LL.set(i, j, Math.min(LL.get(i, j), A.get(i, k) + A.get(k, j)));
				}
			}
		}

		return LL;
	}

	public DenseMatrixInt allPairShortestPaths(DenseMatrixInt A) {
		int n = A.width;
		int m = (int)Math.round(Math.pow(2, Math.ceil(Math.log(n) / Math.log(2))));

		System.out.println(m);
		DenseMatrixInt L = A;

		for (int i = 0; i < m; i++) {
			System.out.println("iteration: " + i + " / " + m);
			L = extendShortestPaths(L);
		}

		return L;
	}

	public DenseMatrixInt floydWarshall(DenseMatrixInt A) {
		int n = A.width;
		for (int i = 0; i < n; i++) {
			System.out.println(i);
			for (int j = 0; j < n; j++) {
				for (int k = 0; k < n; k++) {
					A.set(i, j, Math.min(A.get(i, j), A.get(i, k) + A.get(k, j)));
				}
			}
		}
		return A;
	}
}
