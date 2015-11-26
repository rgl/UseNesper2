package com.example;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventPropertyDescriptor;
import com.espertech.esper.client.UpdateListener;
import org.junit.Test;
import static org.junit.Assert.*;

public class EplTest {

    @Test
    public void groupBy() throws InterruptedException {
        EPServiceProvider engine = createEngine();

        AggregateResultsUpdateListener actualResultsListener = new AggregateResultsUpdateListener();

        EPStatement statement = engine.getEPAdministrator().createEPL(
                "select service, count(*) as value\n"
                + "from Service.win:time_batch(1 seconds)\n"
                + "group by service\n"
                + "order by service");

        String expected = "service:String\n"
                + "value:Long\n"
                + "\n"
                + "#0.0: service:(null)=(null) value:Long=3\n"
                + "#0.1: service:String=Test value:Long=7\n"
                + "\n"
                + "#1.0: service:(null)=(null) value:Long=3\n"
                + "#1.1: service:String=Test value:Long=7\n"
                + "\n"
                + "#2.0: service:(null)=(null) value:Long=0\n"
                + "#2.1: service:String=Test value:Long=0";

        statement.addListener(actualResultsListener);

        for (int n = 0; n < 10; ++n) {
            engine.getEPRuntime().sendEvent(new ServiceEvent(n % 4 == 0 ? null : "Test"));
        }

        Thread.sleep(1500);

        for (int n = 0; n < 10; ++n) {
            engine.getEPRuntime().sendEvent(new ServiceEvent(n % 4 == 0 ? null : "Test"));
        }

        Thread.sleep(2500);

        String actual = String.format(
                "%s\n\n%s",
                getStatementProperties(statement),
                actualResultsListener.toString());

        assertEquals(expected, actual);
    }

    private static class AggregateResultsUpdateListener implements UpdateListener {

        StringBuilder sb = new StringBuilder();
        int n = 0;


        @Override
        public void update(EventBean[] result, EventBean[] ebs) {
            StringBuilder sb = new StringBuilder(this.sb.length() > 0 ? "\n\n" : "");

            for (int i = 0; i < result.length; ++i) {
                EventBean r = result[i];

                if (i > 0) {
                    sb.append('\n');
                }

                sb.append(
                        String.format(
                                "#%d.%d: %s",
                                n,
                                i,
                                formatResult(r)));
            }

            this.sb.append(sb);

            ++n;
        }

        @Override
        public String toString() {
            return sb.toString();
        }
    }

    private static String formatResult(EventBean r) {
        StringBuilder sb = new StringBuilder();

        for (String name : r.getEventType().getPropertyNames()) {
            Object value = r.get(name);

            if (sb.length() > 0) {
                sb.append(' ');
            }

            sb.append(
                    String.format(
                            "%s:%s=%s",
                            name,
                            value != null ? value.getClass().getSimpleName() : "(null)",
                            value != null ? value.toString() : "(null)"));
        }

        return sb.toString();
    }

    private static String getStatementProperties(EPStatement statement) {
        StringBuilder sb = new StringBuilder();

        EventPropertyDescriptor[] propertyDescriptors = statement.getEventType().getPropertyDescriptors();

        for (EventPropertyDescriptor propertyDescriptor : propertyDescriptors) {
            if (sb.length() > 0) {
                sb.append('\n');
            }
            sb.append(
                    String.format(
                            "%s:%s",
                            propertyDescriptor.getPropertyName(),
                            getStatementPropertyType(
                                    propertyDescriptor.getPropertyName(),
                                    propertyDescriptor.getPropertyType())));
        }

        return sb.toString();
    }

    public static String getStatementPropertyType(String propertyName, Class type) {
        if (type.isEnum()) {
            return "Enum";
        }

        if (type.isArray()) {
            return "Array";
        }

        return type.getSimpleName();
    }

    private EPServiceProvider createEngine() {
        Configuration configuration = new Configuration();

        configuration.addEventType("Service", ServiceEvent.class.getName());

        EPServiceProvider engine = EPServiceProviderManager.getProvider("urn:test", configuration);

        engine.initialize();

        return engine;
    }
}
