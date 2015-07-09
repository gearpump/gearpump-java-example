package kafka;

import org.apache.gearpump.Message;
import org.apache.gearpump.streaming.task.StartTime;
import org.apache.gearpump.streaming.task.Task;
import org.apache.gearpump.streaming.task.TaskContext;
import org.apache.gearpump.cluster.UserConfig;


class Split extends Task {

    private TaskContext context;
    private UserConfig userConf;

    public Split(TaskContext taskContext, UserConfig userConf) {
        super(taskContext, userConf);
        this.context = taskContext;
        this.userConf = userConf;
    }

    private Long now() {
        return System.currentTimeMillis();
    }

    public void onStart(StartTime startTime) {
    }

    public void onNext(Message msg) {
        String line = new String((byte[])(msg.msg()));
        String[] words = line.split(" ");
        for (int i = 0; i < words.length; i++) {
            context.output(new Message(words[i], now()));
        }
        self().tell(new Message("next", now()), self());
    }
}
