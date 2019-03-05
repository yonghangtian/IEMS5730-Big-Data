
import java.util.PriorityQueue;
import java.util.Comparator;
public class TestMedian {
    int count = 0;
    PriorityQueue<Integer> minHeap = new PriorityQueue<Integer>();

    PriorityQueue<Integer> maxHeap = new PriorityQueue<Integer>(new Comparator<Integer>(){
        @Override
        public int compare(Integer o1, Integer o2){
            return o2.compareTo(o1);
        }
    });

    public void Insert(Integer num) {

        count++;

        if (count%2 == 1){

            maxHeap.offer(num);
            int maxOfMax = maxHeap.poll();
            minHeap.offer(maxOfMax);
        }else{

            minHeap.offer(num);
            int minOfMin = minHeap.poll();
            maxHeap.offer(minOfMin);
        }
    }

    public Double GetMedian() {
        if (count%2 == 1){
            return new Double((maxHeap.peek()+minHeap.peek())/2);
        }else{
            return new Double(minHeap.peek());

        }
    }


}