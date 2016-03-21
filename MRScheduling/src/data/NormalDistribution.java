package data;

public class NormalDistribution {

	private double miu;
	private double sigma;//the square of the standard deviation
	
	public NormalDistribution(double m, double s)
	{
		this.miu = m;
		this.sigma = s;
	}
	/**
	 * ����������һƪ�ռ��������[0,1]��������㷨
	 * @param r �������ĵ�ַ���������У��Ա�ÿ�ε��ú����������ӵ�ֵ�����򽫵õ���ȫһ�������ݴӶ�ʧȥ�����
	 * ����r���ǳ�ʼ��Ϊ5
	 * @return
	 */
	private double seftRandom(double[] r){
		double base,u,v,p,temp1,temp2,temp3;
		//����
		base = 256.0;
		//�������� uv;
		u = 17.0;
		v = 139.0;
		//������ֵ
		temp1 = u*(r[0])+v;
		//������
		temp2 = (int)(temp1/base);
		//��������,1��base������
		temp3 = temp1 - temp2*base;
		//����������ӣ�Ϊ��һ��ʹ��
		r[0] = temp3;
		//�������ֵ ����ȡ[0,1]�������
		p = r[0]/base;
		return p;
	}

	/**
	 * ��̬�ֲ���������ɷ�
	 * @param u	��̬�ֲ��ľ�ֵ
	 * @param t	��̬�ֲ��ķ���0
	 * @param r	�������
	 * @param n	��̬�ֲ���ʽ��n
	 * @return
	 */
	public double randZT(double u,double t,double[]r,double n){
		int i;
		double total = 0.0;
		double result;
		for(i = 0;i<n;i++){
			//�ۼ�
			total += seftRandom(r);
		}
		//�õ��������
		result = u+t*((total-n/2.0)/Math.sqrt(n/12));
		return result;
	}
	public double getNext()
	{
		/*double[] r = {5.0};
		double n = 12.0;
		return randZT(this.miu, this.sigma, r, n);*/
		double n = 12;
		double x = 0, temp = n;
		do {
			x = 0;
			for (int i = 0; i < n; i++)
				x += Math.random();
			x = (x - temp / 2) / Math.sqrt(temp / 12);
			x = miu + x * Math.sqrt(sigma);
		} while (x <= 0); 
		
		return x;
	}
	public double getMiu() {
		return miu;
	}
	public void setMiu(double miu) {
		this.miu = miu;
	}
	public double getSigma() {
		return sigma;
	}
	public void setSigma(double sigma) {
		this.sigma = sigma;
	}
	public static void main(String[] args) {
		
//		ѭ������
//		for(int i = 0;i<10;i++){
//			System.out.printf("%10.5f\n",randZT(u,t,r,n));
//		}
		System.out.println();
		System.out.println(new NormalDistribution(50,200).getNext());
		System.out.println(new NormalDistribution(100,300).getNext());
		System.out.println(new NormalDistribution(19,145).getNext());
		System.out.println(new NormalDistribution(154,558).getNext());

	}

}

