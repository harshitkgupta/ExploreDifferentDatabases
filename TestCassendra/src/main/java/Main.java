import java.util.Scanner;
 
/** @author Harshit Gupta */
public class Main {
 
  /** @param args */
  public static void main(String[] args) {
    try (Scanner sc = new Scanner(System.in)) {
      int T = sc.nextInt();
      int a[] = new int[100];
      while (T-- > 0) {
        boolean valid = true;
 
        int N = sc.nextInt();
        if (N % 2 != 0) {
          for (int i = 0; i < N; i++) a[i] = sc.nextInt();
          int i=1;
          for (int j=0; i <= (N+1)/2; i++,j++) {
            if (a[j] != i) {
              valid = false;
              break;
            }
          }
          
          
          i-=2;
          for (int j=(N+1)/2; j<N; i--,j++) {
              if (a[j] != i) {
                valid = false;
                break;
              }
            }
        } else {
          valid = false;
          for (int i = 0; i < N; i++) sc.nextInt();
        }
 
        System.out.println(valid ? "yes" : "no");
      }
    }
  }
}