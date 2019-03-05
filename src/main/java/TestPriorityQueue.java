import java.util.Comparator;
import java.util.PriorityQueue;
public class TestPriorityQueue {
    public static void main(String[] args) {
        PriorityQueue<Integer> pq = new PriorityQueue<Integer>(4, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                // 这里的比较是在源码中用于排序的，源码中会调用这个compare方法比较两个值的大小,这里的大小不是实际的大小，
                // 而是相对意义的排序，如果返回正数或0，则认为o1比o2靠前，如果返回负数，则o2比o1考前
                return o2.compareTo(o1);
            }
        });
        pq.add(9);
        pq.add(4);
        pq.add(3);
        pq.add(5);
        pq.add(2);
        System.out.println(pq.size());
        System.out.println(pq.peek());
    }
}
