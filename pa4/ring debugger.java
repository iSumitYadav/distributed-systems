import java.util.Formatter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HelloWorld{
     public static void main(String []args){
        try{
            System.out.println("5562  11124 avd4");
            String n4 = genHash("5562");
            System.out.println(n4);
            
            System.out.println("5556  11112 avd1");
            String n1 = genHash("5556");
            System.out.println(n1);
            
            System.out.println("5554 11108 avd0");
            String n0 = genHash("5554");
            System.out.println(n0);
            
            System.out.println("5558  11116 avd2");
            String n2 = genHash("5558");
            System.out.println(n2);
            
            System.out.println("5560  11120 avd3");
            String n3 = genHash("5560");
            System.out.println(n3);
            
            System.out.println("======================checker");
            String hashedkey = genHash("DG0VH76EWKr6hInXIlifbF68KrCsVfbX");
            System.out.println(hashedkey);
            
            System.out.println(hashedkey.compareTo(n4));
            System.out.println(hashedkey.compareTo(n1));
            System.out.println(hashedkey.compareTo(n0));
            System.out.println(hashedkey.compareTo(n2));
            System.out.println(hashedkey.compareTo(n3));
            
        } catch (Exception e){
            System.out.println("Hello World");
        }
     }
     
    private static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}