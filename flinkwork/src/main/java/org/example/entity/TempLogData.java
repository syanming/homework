package org.example.entity;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

/**
 * log数据
 *
 * @Author ly
 * @Date 2023/03/26,0026,周日 下午 07:40
 */
@Data
@Builder
public class TempLogData implements Serializable {
    private static final long serialVersionUID = -4247592475602467286L;
    private String ip;
    private String userId;
    private String method;
    private String url;

    private Long eventTime;
}
