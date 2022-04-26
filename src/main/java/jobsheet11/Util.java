package jobsheet11;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Util
{
    public static String getMachineName()
    {
        try
        {
            InetAddress addr;
            addr = InetAddress.getLocalHost();
            return addr.getHostName();
        }
        catch (UnknownHostException ex)
        {
            System.out.println("Hostname can not be resolved");
        }

        return "Unknown";
    }
}
