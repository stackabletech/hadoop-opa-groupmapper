package tech.stackable.hadoop;

import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class OpaGroupsMappingTest {

    @Test
    public void getGroupsAlice() {
        OpaGroupsMapping mapper = new OpaGroupsMapping();

        try {
            List<String> result = mapper.getGroups("alice");
            System.out.println("groups: " + result);
            assertArrayEquals(result.toArray(), new String[]{
                    "admin"});
        } catch (IOException e) {
            System.out.println("error: " + e.getMessage());
        }
    }

    @Test
    public void getGroupsBob() {
        OpaGroupsMapping mapper = new OpaGroupsMapping();

        try {
            List<String> result = mapper.getGroups("bob");
            System.out.println("groups: " + result);
            assertArrayEquals(result.toArray(), new String[]{
                    "employee", "billing"});
        } catch (IOException e) {
            System.out.println("error: " + e.getMessage());
        }
    }

    @Test
    public void getGroupsUnknown() {
        OpaGroupsMapping mapper = new OpaGroupsMapping();

        try {
            List<String> result = mapper.getGroups("unknown");
            System.out.println("groups: " + result);
            assertArrayEquals(result.toArray(), new String[]{
                    });
        } catch (IOException e) {
            System.out.println("error: " + e.getMessage());
        }
    }
}