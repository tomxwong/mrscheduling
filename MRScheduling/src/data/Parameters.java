package data;

public class Parameters {
	//used for normal distribution
	private double p_map_num_miu;
	private double p_map_dura_miu;
	private double p_map_num_sig;
	private double p_map_dura_sig;
	
	private double p_reduce_num_miu;
	private double p_reduce_dura_miu;
	private double p_reduce_num_sig;
	private double p_reduce_dura_sig;
	
	//used for uniform distribution
	private double p_task_num_low;
	private double p_map_dura_low;
	private double p_task_num_high;
	private double p_map_dura_high;
	
	//used for scaling factor in both uni and bi model
	private double p_scale_uni_low;
	private double p_scale_uni_high;
	
	private double p_scale_bi_major_low;
	private double p_scale_bi_major_high;
	
	private double p_scale_bi_minor_low;
	private double p_scale_bi_minor_high;
	
	private double p_scale_bi_split_point;
	//for deadline
	private long p_job_deadline_miu;
	private long p_job_deadline_sigma;
	
	public long getP_job_deadline_miu() {
		return p_job_deadline_miu;
	}

	public void setP_job_deadline_miu(long p_job_deadline_miu) {
		this.p_job_deadline_miu = p_job_deadline_miu;
	}

	public long getP_job_deadline_sigma() {
		return p_job_deadline_sigma;
	}

	public void setP_job_deadline_sigma(long p_job_deadline_sigma) {
		this.p_job_deadline_sigma = p_job_deadline_sigma;
	}

	//used for cluster config
	private double p_io_rate;//the rate of input to output of map tasks
	
	private int p_map_input_low;//range of map input size
	private int p_map_input_high;
	
	private int p_node;
	private int p_rack;
	private int p_map_slot;
	private int p_reduce_slot;
	
	private int p_jobs;

	
	private double p_omega;
	public double getP_map_dura_miu() {
		return p_map_dura_miu;
	}

	public void setP_map_dura_miu(double p_map_dura_miu) {
		this.p_map_dura_miu = p_map_dura_miu;
	}


	public double getP_map_dura_sig() {
		return p_map_dura_sig;
	}

	public void setP_map_dura_sig(double p_map_dura_sig) {
		this.p_map_dura_sig = p_map_dura_sig;
	}

	public double getP_task_num_low() {
		return p_task_num_low;
	}

	public void setP_task_num_low(double p_task_num_low) {
		this.p_task_num_low = p_task_num_low;
	}

	public double getP_map_dura_low() {
		return p_map_dura_low;
	}

	public void setP_map_dura_low(double p_map_dura_low) {
		this.p_map_dura_low = p_map_dura_low;
	}

	public double getP_task_num_high() {
		return p_task_num_high;
	}

	public void setP_task_num_high(double p_task_num_high) {
		this.p_task_num_high = p_task_num_high;
	}

	public double getP_map_dura_high() {
		return p_map_dura_high;
	}

	public void setP_map_dura_high(double p_map_dura_high) {
		this.p_map_dura_high = p_map_dura_high;
	}

	public double getP_scale_uni_low() {
		return p_scale_uni_low;
	}

	public void setP_scale_uni_low(double p_scale_uni_low) {
		this.p_scale_uni_low = p_scale_uni_low;
	}

	public double getP_scale_uni_high() {
		return p_scale_uni_high;
	}

	public void setP_scale_uni_high(double p_scale_uni_high) {
		this.p_scale_uni_high = p_scale_uni_high;
	}

	public double getP_scale_bi_major_low() {
		return p_scale_bi_major_low;
	}

	public void setP_scale_bi_major_low(double p_scale_bi_major_low) {
		this.p_scale_bi_major_low = p_scale_bi_major_low;
	}

	public double getP_scale_bi_major_high() {
		return p_scale_bi_major_high;
	}

	public void setP_scale_bi_major_high(double p_scale_bi_major_high) {
		this.p_scale_bi_major_high = p_scale_bi_major_high;
	}

	public double getP_scale_bi_minor_low() {
		return p_scale_bi_minor_low;
	}

	public void setP_scale_bi_minor_low(double p_scale_bi_minor_low) {
		this.p_scale_bi_minor_low = p_scale_bi_minor_low;
	}

	public double getP_scale_bi_minor_high() {
		return p_scale_bi_minor_high;
	}

	public void setP_scale_bi_minor_high(double p_scale_bi_minor_high) {
		this.p_scale_bi_minor_high = p_scale_bi_minor_high;
	}

	public double getP_scale_bi_split_point() {
		return p_scale_bi_split_point;
	}

	public void setP_scale_bi_split_point(double p_scale_bi_split_point) {
		this.p_scale_bi_split_point = p_scale_bi_split_point;
	}

	public double getP_io_rate() {
		return p_io_rate;
	}

	public void setP_io_rate(double p_io_rate) {
		this.p_io_rate = p_io_rate;
	}

	public int getP_map_input_low() {
		return p_map_input_low;
	}

	public void setP_map_input_low(int p_map_input_low) {
		this.p_map_input_low = p_map_input_low;
	}

	public int getP_map_input_high() {
		return p_map_input_high;
	}

	public void setP_map_input_high(int p_map_input_high) {
		this.p_map_input_high = p_map_input_high;
	}

	public int getP_node() {
		return p_node;
	}

	public void setP_node(int p_node) {
		this.p_node = p_node;
	}

	public int getP_map_slot() {
		return p_map_slot;
	}

	public void setP_map_slot(int p_map_slot) {
		this.p_map_slot = p_map_slot;
	}

	public int getP_reduce_slot() {
		return p_reduce_slot;
	}

	public void setP_reduce_slot(int p_reduce_slot) {
		this.p_reduce_slot = p_reduce_slot;
	}

	public double getP_map_num_miu() {
		return p_map_num_miu;
	}

	public void setP_map_num_miu(double p_map_num_miu) {
		this.p_map_num_miu = p_map_num_miu;
	}

	public double getP_map_num_sig() {
		return p_map_num_sig;
	}

	public void setP_map_num_sig(double p_map_num_sig) {
		this.p_map_num_sig = p_map_num_sig;
	}

	public double getP_reduce_num_miu() {
		return p_reduce_num_miu;
	}

	public void setP_reduce_num_miu(double p_reduce_num_miu) {
		this.p_reduce_num_miu = p_reduce_num_miu;
	}

	public double getP_reduce_dura_miu() {
		return p_reduce_dura_miu;
	}

	public void setP_reduce_dura_miu(double p_reduce_dura_miu) {
		this.p_reduce_dura_miu = p_reduce_dura_miu;
	}

	public double getP_reduce_num_sig() {
		return p_reduce_num_sig;
	}

	public void setP_reduce_num_sig(double p_reduce_num_sig) {
		this.p_reduce_num_sig = p_reduce_num_sig;
	}

	public double getP_reduce_dura_sig() {
		return p_reduce_dura_sig;
	}

	public void setP_reduce_dura_sig(double p_reduce_dura_sig) {
		this.p_reduce_dura_sig = p_reduce_dura_sig;
	}

	public void setP_rack(int p_rack) {
		this.p_rack = p_rack;
	}

	public int getP_rack() {
		return p_rack;
	}

	public void setP_jobs(int p_jobs) {
		this.p_jobs = p_jobs;
	}

	public int getP_jobs() {
		return p_jobs;
	}

	public void setP_omega(double p_omega) {
		this.p_omega = p_omega;
	}

	public double getP_omega() {
		return p_omega;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
