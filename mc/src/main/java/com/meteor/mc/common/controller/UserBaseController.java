package com.meteor.mc.common.controller;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.propertyeditors.CustomDateEditor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.ServletRequestDataBinder;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.servlet.mvc.SimpleFormController;

import com.meteor.mc.controller.LoginController;

/**
 * 
 * 用户相关Action父类
 * @author 
 *
 */
@Controller
public class UserBaseController<T> extends SimpleFormController {

	private static ThreadLocal<HttpServletRequest> request = new ThreadLocal<HttpServletRequest>();

	private static ThreadLocal<HttpServletResponse> response = new ThreadLocal<HttpServletResponse>();

	protected ModelMap model;

	@InitBinder
	protected void initBinder(HttpServletRequest request, ServletRequestDataBinder binder) throws Exception {
		DateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		CustomDateEditor dateEditor = new CustomDateEditor(fmt, true);
		binder.registerCustomEditor(Date.class, dateEditor);
		super.initBinder(request, binder);
	}

	@SuppressWarnings("static-access")
	@ModelAttribute("request")
	public void setRequest(HttpServletRequest request) {
		this.request.set(request);
	}

	@SuppressWarnings("static-access")
	@ModelAttribute("response")
	public void setResponse(HttpServletResponse response) {
		this.response.set(response);
	}

	@ModelAttribute("model")
	public ModelMap setModel(ModelMap model) {
		this.model = model;
		return model;
	}

	/**
	 * 从request获取指定cookie的值
	 * @param request
	 * @param cookieName
	 * @return
	 */
	public final static String getCookieValue(HttpServletRequest request, String cookieName) {
		if (StringUtils.isBlank(cookieName))
			return null;
		Cookie[] cookies = request.getCookies();
		for (Cookie cookie : cookies) {
			if (cookieName.equals(cookie.getName())) {
				return cookie.getValue();
			}
		}
		return null;
	}
	
	public String getLoginPassport() {
		return getLoginPassport(getRequest());
		
	}
	
	public static String getLoginPassport(HttpServletRequest request) {
		String userAgent = request.getHeader("user-agent");
		String cookie = getCookieValue(request, LoginController.ADMIN_IN_COOKIE_KEY);
		if(StringUtils.isBlank(cookie)) {
			return null;
		}
		String[] cookieArr = StringUtils.split(cookie, LoginController.COOKIE_SPLIT_FLAG);
		if(cookieArr.length != 3) {
			return null;
		}
		String passport = cookieArr[0];
		String time = cookieArr[1];
		String sign = cookieArr[2];
		
		if (DigestUtils.md5Hex(passport + time + userAgent + LoginController.SIGN).equals(sign)) {
			return passport;
		}
		
		return null;
		
	}

	/**
	 * 返回HttpServletRequest
	 * @return
	 */
	public HttpServletRequest getHttpServletRequest() {
		return getRequest();
	}

	/**
	 * 返回HttpServletResponse
	 * @return
	 */
	public HttpServletResponse getHttpServletResponse() {
		return getResponse();
	}

	/**
	 * 设置request的属性
	 * @param key
	 * @param value
	 */
	public void setRequestAttribute(String key, Object value) {
		getRequest().setAttribute(key, value);
	}

	protected HttpServletRequest getRequest() {
		return request.get();
	}

	protected HttpServletResponse getResponse() {
		return response.get();
	}

}
