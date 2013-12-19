import java.util.Arrays;

public class DenseMatrixInt {

	public final int[][] array;
	public final int width;
	public final int height;

	public DenseMatrixInt(int width, int height) {
		assert width > 0;
		assert height > 0;
		this.array = new int[height][width];
		this.height = height;
		this.width = width;
	}

	public void fill(int value) {
		for (int i = 0; i < height; i++) {
			Arrays.fill(array[i], value);
		}
	}

	public int get(int i, int j) {
		return array[i][j];
	}

	public void set(int i, int j, int v) {
		array[i][j] = v;
	}

}
