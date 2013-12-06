package dgm.driver.handler;

import dgm.ID;
import dgm.degraphmalizr.degraphmalize.DegraphmalizeRequestScope;
import dgm.degraphmalizr.degraphmalize.DegraphmalizeRequestType;
import dgm.degraphmalizr.degraphmalize.JobRequest;
import dgm.exceptions.DegraphmalizerException;
import dgm.exceptions.InvalidRequest;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;

/**
 * Transform a HttpRequest into
 */
public class DegraphmalizeDecoder extends OneToOneDecoder {
    @Override
    protected final Object decode(ChannelHandlerContext channelHandlerContext, Channel channel, Object o) throws DegraphmalizerException {
        final HttpRequest request = (HttpRequest) o;
        final DegraphmalizeRequestType requestType = actionTypeFor(request);

        // split url /TYPE/ID/ or fail
        final String[] components = urlDecode(request.getUri().substring(1).split("/"));

        switch (requestType){
            case DELETE:
                if (components.length < 1 || components.length > 4) {
                    throw new InvalidRequest("DELETE URL " + request.getUri() + " must be of the form '/{index}/{type}/{id}/{version}'");
                }
                break;
            case UPDATE:
                if (components.length != 4) {
                    throw new InvalidRequest("UPDATE URL " + request.getUri() + " must be of the form '/{index}/{type}/{id}/{version}'");
                }
                break;
            default:
                throw new InvalidRequest("Unsupported operation: " + requestType);

        }

        return new JobRequest(requestType, actionScopeFor(components), getID(components));
    }

    private static String[] urlDecode(String... components) {
        String[] result = new String[components.length];
        for (int i = 0; i < result.length; i++) {
            try {
                result[i] = URLDecoder.decode(components[i], "UTF-8");
            } catch (UnsupportedEncodingException e) {
                result[i] = components[i];
            }
        }
        return result;
    }

    // HTTP.method ? DELETE => anti-degraphmalize it
    private static DegraphmalizeRequestType actionTypeFor(HttpRequest req) {
        if (HttpMethod.DELETE.equals(req.getMethod())) {
            return DegraphmalizeRequestType.DELETE;
        }

        return DegraphmalizeRequestType.UPDATE;
    }

    private static DegraphmalizeRequestScope actionScopeFor(String[] components) {
        DegraphmalizeRequestScope requestScope = DegraphmalizeRequestScope.DOCUMENT;
        switch (components.length) {
            case 4:
                requestScope = DegraphmalizeRequestScope.DOCUMENT;
                break;
            case 3:
                requestScope = DegraphmalizeRequestScope.DOCUMENT_ANY_VERSION;
                break;
            case 2:
                requestScope = DegraphmalizeRequestScope.TYPE_IN_INDEX;
                break;
            case 1:
                requestScope = DegraphmalizeRequestScope.INDEX;
                break;
        }
        return requestScope;
    }


    private static ID getID(String[] components) {
        long version = 0;
        String id = null;
        String type = null;
        String index;

        switch (components.length) {
            case 4:
                version = Long.parseLong(components[3]);
            case 3:
                id = components[2];
            case 2:
                type = components[1];
            case 1:
                index = components[0];
                break;
            default:
                throw new InvalidRequest("Invalid number of components in the request ");
        }
        return new ID(index, type, id, version);
    }
}
