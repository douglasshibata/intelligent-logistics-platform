package com.logistics.security;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;
import org.casbin.jcasbin.main.Enforcer;

/**
 * CasbinSecurityInterceptor
 *
 * An enterprise Apache Dubbo 3 RPC interceptor (Filter) enforcing multi-tenant,
 * role-and-attribute-based (RBAC/ABAC) security controls at the microservice boundary.
 * Intercepts incoming logistics requests, extracts tenant, user, and action metadata,
 * and delegates policy evaluations to an embedded Casbin engine.
 */
@Activate(group = {CommonConstants.PROVIDER})
public class CasbinSecurityInterceptor implements Filter {

    private final Enforcer enforcer;

    public CasbinSecurityInterceptor() {
        // Initialize the Casbin Enforcer using the localized PERM configuration
        String modelPath = "intelligent-logistics-platform/security/casbin_model.conf";
        String policyPath = "intelligent-logistics-platform/security/casbin_policy.csv";
        this.enforcer = new Enforcer(modelPath, policyPath);
    }

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        // 1. Retrieve the multi-tenancy security context from Dubbo metadata attachments
        String tenantId = invocation.getAttachment("tenant_id"); // e.g., "hospital_supplies", "retail_dry"
        String userId = invocation.getAttachment("user_id");     // e.g., "USR-1092"
        String userRole = invocation.getAttachment("user_role");   // e.g., "logistic_agent", "driver", "operator"

        // 2. Map service invocation details to target resource path and action type
        String serviceName = invoker.getInterface().getName(); // e.g., "com.logistics.api.FleetAllocationService"
        String methodName = invocation.getMethodName();       // e.g., "reallocateVehicle"
        String resourcePath = serviceName + "/" + methodName;

        // Map method execution signature to CRUD permission attributes (ABAC)
        String actionType = "READ";
        if (methodName.startsWith("save") ||
            methodName.startsWith("create") ||
            methodName.startsWith("update") ||
            methodName.startsWith("delete") ||
            methodName.startsWith("reallocate")) {
            actionType = "WRITE";
        }

        // 3. Fallback check for missing or unauthenticated contextual headers
        if (tenantId == null || userRole == null || userId == null) {
            System.err.println(String.format(
                "[CASBIN DENIED] Blocked request to %s due to missing authentication attachments.",
                resourcePath
            ));
            throw new RpcException(RpcException.FORBIDDEN_EXCEPTION,
                "Access Denied: Multi-tenant security context attachments (tenant_id, user_role, user_id) are missing.");
        }

        // 4. Evaluate access rules: enforcer.enforce(userRole, tenantId, resourcePath, actionType)
        boolean authorized = enforcer.enforce(userRole, tenantId, resourcePath, actionType);

        if (!authorized) {
            System.err.println(String.format(
                "[CASBIN SECURITY VIOLATION] Tenant: %s | User: %s (%s) tried to invoke %s [%s] but authorization was REJECTED.",
                tenantId, userId, userRole, resourcePath, actionType
            ));
            throw new RpcException(RpcException.FORBIDDEN_EXCEPTION,
                "Security Exception: User role is not authorized to access the requested resource inside this tenant domain.");
        }

        System.out.println(String.format(
            "[CASBIN APPROVED] Tenant: %s | User: %s (%s) authorized to invoke %s [%s].",
            tenantId, userId, userRole, resourcePath, actionType
        ));

        // 5. Access validated; proceed down the Dubbo interceptor filter chain
        return invoker.invoke(invocation);
    }
}
