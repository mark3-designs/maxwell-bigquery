package com.steckytech.maxwell;

public class Util {

    private static final Util instance = new Util();

    public static Object instantiateObject(String className) throws ClassNotFoundException, InstantiationException, IllegalAccessError, IllegalAccessException, LinkageError {
        Class c= null;

        Object obj= null;

        try {
            c= Class.forName(className, true, instance.getClass().getClassLoader());

            if (c != null) {
                obj= c.newInstance();
            } else {
                String errMsg= "class.forName() returned null when attempting to load '"+ className +"'";
                throw new InstantiationException(errMsg);
            }

        } catch (ClassNotFoundException cnfx) {
            String errMsg= "unkown resource '"+ className +"' [ClassNotFoundException]";
            throw new ClassNotFoundException(errMsg, cnfx);
        } catch (InstantiationException ix) {
            String errMsg= "could not instantiate '"+ className +"' ["+ ix.getMessage() +"]";
            throw new InstantiationException(errMsg);
        } catch (IllegalAccessException iax) {
            String errMsg= "access denied '"+ className +"' ["+ iax.getMessage() +"]";
            throw new IllegalAccessException(errMsg);
        } catch (ExceptionInInitializerError eiie) {
            String errMsg= eiie.getMessage();
            throw new ExceptionInInitializerError(errMsg);
        } catch (LinkageError le) {
            String errMsg= le.getMessage();
            throw new LinkageError(errMsg);
        }

        return obj;
    }

}
